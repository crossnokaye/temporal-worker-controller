// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// Grouped retirement waits for an application-owned workflow to certify that no
// new work can enter a build and no caller relies on implicit ramp selection.
// The controller can then initialize server-owned drainage with a zero-percent
// ramp. Only Temporal drainage permits pod shutdown; the application check does not.
// After pods stop, an optional saved cleanup workflow must complete before any
// Temporal version, Deployment object, or saved build record can be deleted.
package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type (
	// groupRetirement retains the observed members while execution checks the
	// saved retirement workflow. It exists only for this reconciliation.
	groupRetirement struct {
		buildID string
		saved   *k8s.GroupRevision
		actual  map[string]*appsv1.Deployment
		desired map[string]*appsv1.Deployment
		version *temporal.VersionInfo
	}
)

// retirementWorkflowID assigns one workflow identity to a saved build. Continuing
// as new keeps this identity, so each describe reads the latest execution.
func retirementWorkflowID(deploymentName, buildID string) string {
	return fmt.Sprintf("retire-%s:%s", deploymentName, buildID)
}

// checkTargetGroupRetirement rejects reselecting a build once its retirement
// workflow exists. The application may already have closed admission while that
// workflow waits for old work, so restoring routing would violate its promise.
func checkTargetGroupRetirement(ctx context.Context, c sdkclient.Client, w *v1alpha1.WorkerDeployment, p *plan) error {
	if p.targetGroup == nil {
		return nil
	}
	buildID := k8s.ComputeBuildID(w)
	id := retirementWorkflowID(p.WorkerDeploymentName, buildID)
	_, err := c.DescribeWorkflowExecution(ctx, id, "")
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check selected group retirement %q: %w", id, err)
	}
	return fmt.Errorf("grouped build %q has started retirement; use a new build ID", buildID)
}

// planGroupRetirements waits for the saved application approval and closed
// readiness workflows. It starts server-owned drainage for inactive builds, then
// retains every member until Temporal reports the build drained.
func (r *WorkerDeploymentReconciler) planGroupRetirements(ctx context.Context, c sdkclient.Client, p *plan) error {
	var failures []error
	for _, group := range p.groupRetirements {
		if err := r.planGroupRetirement(ctx, c, p, group); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// planGroupRetirement keeps one obsolete group's saved pod counts until its
// application approval, readiness workflow closure, and Temporal drainage permit
// retirement. Once workers stop, an optional application cleanup must succeed
// before any member or saved record is nominated for deletion.
func (r *WorkerDeploymentReconciler) planGroupRetirement(ctx context.Context, c sdkclient.Client, p *plan, group groupRetirement) error {
	approved, err := completedRetirement(ctx, c, p, group)
	if err != nil {
		return err
	}
	if !approved {
		return nil
	}
	closed, err := groupReadinessGatesClosed(ctx, c, p.WorkerDeploymentName, group)
	if err != nil {
		return err
	}
	if !closed {
		return nil
	}
	if group.version == nil || group.version.Status == v1alpha1.VersionStatusNotRegistered || group.version.Status == v1alpha1.VersionStatusInactive || group.version.Status == v1alpha1.VersionStatusCreated || group.version.Status == v1alpha1.VersionStatusRamping {
		if p.allowGroupedDrainage && p.UpdateVersionConfig == nil {
			if err := initializeGroupDrainage(ctx, c, p, group.buildID); err != nil {
				return err
			}
		}
		return nil
	}
	if group.version.Status != v1alpha1.VersionStatusDrained || group.version.DrainedSince == nil {
		return nil
	}
	elapsed := time.Since(*group.version.DrainedSince)
	if elapsed <= group.saved.Spec.SunsetStrategy.ScaledownDelay.Duration {
		return nil
	}
	deleting := elapsed > group.saved.Spec.SunsetStrategy.ScaledownDelay.Duration+group.saved.Spec.SunsetStrategy.DeleteDelay.Duration && groupStopped(group.actual, group.desired)
	for _, desired := range p.CreateDeployments {
		if desired.Labels[k8s.BuildIDLabel] == group.buildID {
			zero := int32(0)
			desired.Spec.Replicas = &zero
		}
	}
	for _, actual := range group.actual {
		for ref := range p.ScaleDeployments {
			if ref.Name == actual.Name && ref.Namespace == actual.Namespace {
				delete(p.ScaleDeployments, ref)
			}
		}
		if actual.Spec.Replicas == nil || *actual.Spec.Replicas != 0 {
			p.ScaleDeployments[k8s.NewObjectRef(actual)] = 0
		}
	}
	if !deleting {
		return nil
	}
	if config := group.saved.Spec.GroupCleanupWorkflow; config != nil {
		stopped, err := r.groupPodsGone(ctx, group)
		if err != nil || !stopped {
			return err
		}
		completed, err := completedGroupWorkflow(ctx, c, p, group.buildID,
			cleanupWorkflowID(p.WorkerDeploymentName, group.buildID), config.WorkflowType, config.TaskQueue)
		if err != nil || !completed {
			return err
		}
	}
	for _, actual := range group.actual {
		p.DeleteDeployments = append(p.DeleteDeployments, actual)
	}
	p.MarkRevisionsForDeletion = append(p.MarkRevisionsForDeletion, group.saved.Object)
	return nil
}

// groupPodsGone reads each saved member's exact pod selector directly from
// Kubernetes. Any remaining pod, including one still terminating, keeps the
// application cleanup waiting after the Deployment reports zero replicas.
func (r *WorkerDeploymentReconciler) groupPodsGone(ctx context.Context, group groupRetirement) (bool, error) {
	for _, member := range group.desired {
		selector, err := metav1.LabelSelectorAsSelector(member.Spec.Selector)
		if err != nil {
			return false, fmt.Errorf("read grouped member %q pod selector: %w", member.Name, err)
		}
		var pods corev1.PodList
		if err := r.apiReader.List(ctx, &pods, client.InNamespace(member.Namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
			return false, fmt.Errorf("confirm grouped member %q pods stopped: %w", member.Name, err)
		}
		if len(pods.Items) != 0 {
			return false, nil
		}
	}
	return true, nil
}

// cleanupWorkflowID assigns application cleanup its own identity for one saved
// build. Reconciliation observes this execution again after an interrupted start.
func cleanupWorkflowID(deploymentName, buildID string) string {
	return fmt.Sprintf("cleanup-%s:%s", deploymentName, buildID)
}

// initializeGroupDrainage briefly assigns an approved obsolete build a zero
// percent ramp and clears that exact assignment. Each write uses a fresh owner
// token, so concurrent routing changes fail rather than being overwritten. A
// later reconcile can finish a zero percent assignment left by a stopped process.
func initializeGroupDrainage(ctx context.Context, c sdkclient.Client, p *plan, buildID string) error {
	handle := c.WorkerDeploymentClient().GetHandle(p.WorkerDeploymentName)
	description, err := handle.Describe(ctx, sdkclient.WorkerDeploymentDescribeOptions{})
	if err != nil {
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			return nil
		}
		return fmt.Errorf("describe routing before group retirement: %w", err)
	}
	routing := description.Info.RoutingConfig
	if routing.CurrentVersion != nil && routing.CurrentVersion.BuildID == buildID {
		return nil
	}
	if routing.RampingVersionPercentage != 0 || (routing.RampingVersion != nil && routing.RampingVersion.BuildID != buildID) {
		return nil
	}
	if len(description.ConflictToken) == 0 {
		return fmt.Errorf("group retirement requires a nonempty routing conflict token")
	}
	token := description.ConflictToken
	if routing.RampingVersion == nil {
		response, err := handle.SetRampingVersion(ctx, sdkclient.WorkerDeploymentSetRampingVersionOptions{
			BuildID: buildID, Percentage: 0, ConflictToken: token,
			Identity: getControllerIdentity(), IgnoreMissingTaskQueues: true, AllowNoPollers: true,
		})
		if err != nil {
			return fmt.Errorf("initialize drainage for grouped build %q: %w", buildID, err)
		}
		token = response.ConflictToken
	}
	if len(token) == 0 {
		return fmt.Errorf("group retirement requires a nonempty updated routing conflict token")
	}
	_, err = handle.SetRampingVersion(ctx, sdkclient.WorkerDeploymentSetRampingVersionOptions{
		BuildID: "", Percentage: 0, ConflictToken: token, Identity: getControllerIdentity(),
	})
	if err != nil {
		return fmt.Errorf("clear retirement ramp for grouped build %q: %w", buildID, err)
	}
	return nil
}

// completedRetirement starts the saved application check once and reads its
// latest run thereafter. Success authorizes the routing maintenance operation;
// Temporal must still prove that every pinned execution has finished.
func completedRetirement(ctx context.Context, c sdkclient.Client, p *plan, group groupRetirement) (bool, error) {
	config := group.saved.Spec.GroupRetirementWorkflow
	return completedGroupWorkflow(ctx, c, p, group.buildID,
		retirementWorkflowID(p.WorkerDeploymentName, group.buildID), config.WorkflowType, config.TaskQueue)
}

// completedGroupWorkflow starts one saved application workflow and accepts only
// a successful execution with the exact type, queue, and original two arguments.
// Missing or running workflows keep deletion waiting; failed requests return an
// error so reconciliation retains the saved build and retries observation later.
func completedGroupWorkflow(ctx context.Context, c sdkclient.Client, p *plan, buildID, id, workflowType, taskQueue string) (bool, error) {
	deploymentName := p.WorkerDeploymentName
	description, err := c.DescribeWorkflowExecution(ctx, id, "")
	var missing *serviceerror.NotFound
	if errors.As(err, &missing) {
		_, err = c.ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID: id, TaskQueue: taskQueue,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
		}, workflowType, deploymentName, buildID)
		var exists *serviceerror.WorkflowExecutionAlreadyStarted
		if err != nil && !errors.As(err, &exists) {
			return false, fmt.Errorf("start group workflow %q: %w", id, err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("describe group workflow %q: %w", id, err)
	}
	info := description.GetWorkflowExecutionInfo()
	if info.GetExecution().GetWorkflowId() != id || info.GetExecution().GetRunId() == "" || info.GetType().GetName() != workflowType || info.GetTaskQueue() != taskQueue || !unversionedGroupWorkflow(info.GetVersioningInfo()) {
		return false, fmt.Errorf("group workflow %q must match its saved type and unversioned management queue", id)
	}
	if info.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED || info.GetCloseTime() == nil || info.GetCloseTime().CheckValid() != nil {
		return false, nil
	}
	// Request a small first history page for this exact run. Temporal can return
	// a whole stored event batch; validate its first event to prove which
	// deployment and build this execution received.
	history, err := c.WorkflowService().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              p.TemporalNamespace,
		Execution:              &commonpb.WorkflowExecution{WorkflowId: id, RunId: info.GetExecution().GetRunId()},
		MaximumPageSize:        1,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	})
	if err != nil {
		return false, fmt.Errorf("read group workflow input %q: %w", id, err)
	}
	events := history.GetHistory().GetEvents()
	if len(events) == 0 || events[0].GetEventId() != 1 || events[0].GetWorkflowExecutionStartedEventAttributes() == nil {
		return false, fmt.Errorf("group workflow %q has no initial input event", id)
	}
	input := events[0].GetWorkflowExecutionStartedEventAttributes().GetInput()
	if len(input.GetPayloads()) != 2 {
		return false, fmt.Errorf("group workflow %q requires exactly two string arguments", id)
	}
	var suppliedDeployment, suppliedBuild string
	if err := converter.GetDefaultDataConverter().FromPayloads(input, &suppliedDeployment, &suppliedBuild); err != nil {
		return false, fmt.Errorf("decode group workflow input %q: %w", id, err)
	}
	if suppliedDeployment != deploymentName || suppliedBuild != buildID {
		return false, fmt.Errorf("group workflow %q input does not match its saved deployment and build", id)
	}
	return true, nil
}

// unversionedGroupWorkflow rejects effective deployment assignments as well as
// overrides, so a grouped worker cannot approve its own retirement or cleanup.
func unversionedGroupWorkflow(info *workflowpb.WorkflowExecutionVersioningInfo) bool {
	//lint:ignore SA1019 Older servers expose deployment assignments in these fields.
	return info.GetBehavior() == enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED && info.GetVersioningOverride() == nil && info.GetDeploymentVersion() == nil && info.GetVersionTransition() == nil && info.GetDeployment() == nil && info.GetVersion() == "" && info.GetDeploymentTransition() == nil
}

// groupReadinessGatesClosed reads the latest execution for each saved readiness
// gate after cancellation. An asynchronous cancellation request is not closure;
// a running or continuing gate keeps every grouped member alive.
func groupReadinessGatesClosed(ctx context.Context, c sdkclient.Client, deploymentName string, group groupRetirement) (bool, error) {
	if group.saved.Spec.RolloutStrategy.Gate == nil {
		return true, nil
	}
	for _, queue := range group.saved.Spec.ExpectedTaskQueues {
		if !slices.Contains(queue.Kinds, v1alpha1.TaskQueueWorkflow) {
			continue
		}
		id := temporal.GetTestWorkflowID(deploymentName, group.buildID, queue.Name)
		description, err := c.DescribeWorkflowExecution(ctx, id, "")
		var missing *serviceerror.NotFound
		if errors.As(err, &missing) {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("confirm superseded readiness workflow %q closed: %w", id, err)
		}
		info := description.GetWorkflowExecutionInfo()
		if info.GetExecution().GetWorkflowId() != id || info.GetExecution().GetRunId() == "" || info.GetType().GetName() != group.saved.Spec.RolloutStrategy.Gate.WorkflowType || info.GetTaskQueue() != queue.Name || !gatePinnedToBuild(info.GetVersioningInfo().GetVersioningOverride(), deploymentName, group.buildID) {
			return false, fmt.Errorf("superseded readiness workflow %q does not match its saved settings", id)
		}
		switch info.GetStatus() {
		case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		default:
			return false, nil
		}
	}
	return true, nil
}
