// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/types/known/timestamppb"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func cleanupPlan(t *testing.T) (*plan, *retirementClient) {
	t.Helper()
	p, h := retirementPlan(t, v1alpha1.VersionStatusDrained)
	group := &p.groupRetirements[0]
	worker := groupedWorker(group.buildID)
	worker.Spec = *group.saved.Spec.DeepCopy()
	worker.Spec.GroupCleanupWorkflow = &v1alpha1.GroupCleanupWorkflowConfig{WorkflowType: "CleanupBuild", TaskQueue: "management"}
	r, _ := newTestReconciler(nil)
	saved, err := k8s.NewGroupRevision(worker, group.buildID, r.Scheme)
	require.NoError(t, err)
	group.saved = saved
	drained := time.Now().Add(-time.Hour)
	group.version.DrainedSince = &drained
	for _, member := range group.actual {
		zero := int32(0)
		member.Spec.Replicas = &zero
		member.Status.ObservedGeneration = member.Generation
	}
	c := approvedRetirements(t, p, newStubTemporalClientWithHandle(h))
	return p, c
}

func completeCleanup(t *testing.T, p *plan, c *retirementClient) {
	t.Helper()
	group := p.groupRetirements[0]
	id := cleanupWorkflowID(p.WorkerDeploymentName, group.buildID)
	c.info[id] = &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "cleanup-run"},
		Type:      &commonpb.WorkflowType{Name: group.saved.Spec.GroupCleanupWorkflow.WorkflowType},
		TaskQueue: group.saved.Spec.GroupCleanupWorkflow.TaskQueue,
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		CloseTime: timestamppb.Now(),
	}
	input, err := converter.GetDefaultDataConverter().ToPayloads(p.WorkerDeploymentName, group.buildID)
	require.NoError(t, err)
	c.history.input = input
}

func TestGroupedCleanupWaitsForDrainAndObservedStoppedPods(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*groupRetirement)
		pod    bool
	}{
		{name: "pre-drain retirement still running", change: func(g *groupRetirement) { g.version.Status = v1alpha1.VersionStatusInactive }},
		{name: "draining with stale timestamp", change: func(g *groupRetirement) { g.version.Status = v1alpha1.VersionStatusDraining }},
		{name: "no drainage timestamp", change: func(g *groupRetirement) { g.version.DrainedSince = nil }},
		{name: "scale delay", change: func(g *groupRetirement) {
			g.saved.Spec.SunsetStrategy.ScaledownDelay = &metav1.Duration{Duration: 2 * time.Hour}
		}},
		{name: "delete delay", change: func(g *groupRetirement) {
			g.saved.Spec.SunsetStrategy.DeleteDelay = &metav1.Duration{Duration: 2 * time.Hour}
		}},
		{name: "scale requested but not applied", change: func(g *groupRetirement) { one := int32(1); g.actual["primary"].Spec.Replicas = &one }},
		{name: "scale not observed", change: func(g *groupRetirement) { g.actual["primary"].Generation = 3 }},
		{name: "remaining replicas", change: func(g *groupRetirement) { g.actual["primary"].Status.Replicas = 1 }},
		{name: "terminating replicas", change: func(g *groupRetirement) { one := int32(1); g.actual["primary"].Status.TerminatingReplicas = &one }},
		{name: "terminating pod absent from deployment counters", change: func(*groupRetirement) {}, pod: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, c := cleanupPlan(t)
			group := &p.groupRetirements[0]
			test.change(group)
			objects := []client.Object{}
			if test.name == "pre-drain retirement still running" {
				c.info[retirementWorkflowID(p.WorkerDeploymentName, group.buildID)].Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}
			if test.pod {
				member := group.desired["activities"]
				objects = append(objects, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "still-terminating", Namespace: member.Namespace, Labels: member.Spec.Selector.MatchLabels, DeletionTimestamp: &metav1.Time{Time: time.Now()}, Finalizers: []string{"example.invalid/cleanup"}}})
			}
			r, _ := newTestReconciler(objects)
			require.NoError(t, r.planGroupRetirements(context.Background(), c, p))
			assert.Empty(t, c.started)
			assert.Empty(t, p.DeleteDeployments)
			assert.Empty(t, p.MarkRevisionsForDeletion)
		})
	}
}

func TestGroupedCleanupStartIsStableAndRecoversUncertainReply(t *testing.T) {
	for _, startErr := range []error{nil, errors.New("reply lost"), &serviceerror.WorkflowExecutionAlreadyStarted{}} {
		t.Run("start", func(t *testing.T) {
			p, c := cleanupPlan(t)
			c.startErr = startErr
			r, _ := newTestReconciler(nil)
			err := r.planGroupRetirements(context.Background(), c, p)
			var exists *serviceerror.WorkflowExecutionAlreadyStarted
			assert.Equal(t, startErr != nil && !errors.As(startErr, &exists), err != nil)
			require.Len(t, c.started, 1)
			options := c.started[0]
			assert.Equal(t, cleanupWorkflowID(p.WorkerDeploymentName, "build-b"), options.ID)
			assert.Equal(t, "management", options.TaskQueue)
			assert.Equal(t, "CleanupBuild", c.workflowTypes[0])
			assert.Equal(t, []any{p.WorkerDeploymentName, "build-b"}, c.args[0])
			assert.Zero(t, options.WorkflowExecutionTimeout)
			assert.Zero(t, options.WorkflowRunTimeout)
			assert.Nil(t, options.VersioningOverride)
			assert.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE, options.WorkflowIDReusePolicy)
			assert.Equal(t, enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL, options.WorkflowIDConflictPolicy)
			assert.Empty(t, p.DeleteDeployments)
			assert.Empty(t, p.MarkRevisionsForDeletion)

			completeCleanup(t, p, c)
			require.NoError(t, r.planGroupRetirements(context.Background(), c, p))
			assert.Len(t, c.started, 1)
			assert.Len(t, p.DeleteDeployments, 2)
			assert.Len(t, p.MarkRevisionsForDeletion, 1)
		})
	}
}

func TestGroupedCleanupInvalidOrUnfinishedWorkflowRetainsProof(t *testing.T) {
	for _, test := range []struct {
		name      string
		change    func(*workflowpb.WorkflowExecutionInfo, *retirementClient, *plan)
		wantError bool
	}{
		{"running", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) {
			i.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		}, false},
		{"failed", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) {
			i.Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
		}, false},
		{"canceled", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) {
			i.Status = enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
		}, false},
		{"missing close time", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) { i.CloseTime = nil }, false},
		{"wrong identity", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) {
			i.Execution.WorkflowId = "unrelated"
		}, true},
		{"missing run", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) { i.Execution.RunId = "" }, true},
		{"wrong type", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) { i.Type.Name = "OtherWorkflow" }, true},
		{"wrong queue", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) { i.TaskQueue = "other" }, true},
		{"versioned", func(i *workflowpb.WorkflowExecutionInfo, _ *retirementClient, _ *plan) {
			i.VersioningInfo = &workflowpb.WorkflowExecutionVersioningInfo{Behavior: enumspb.VERSIONING_BEHAVIOR_PINNED}
		}, true},
		{"wrong original arguments", func(_ *workflowpb.WorkflowExecutionInfo, c *retirementClient, p *plan) {
			input, err := converter.GetDefaultDataConverter().ToPayloads(p.WorkerDeploymentName, "other-build")
			require.NoError(t, err)
			id := cleanupWorkflowID(p.WorkerDeploymentName, "build-b")
			c.history.inputs = map[string]*commonpb.Payloads{id: input, retirementWorkflowID(p.WorkerDeploymentName, "build-b"): c.history.input}
		}, true},
		{"history unavailable", func(_ *workflowpb.WorkflowExecutionInfo, c *retirementClient, p *plan) {
			c.history.errors = map[string]error{cleanupWorkflowID(p.WorkerDeploymentName, "build-b"): errors.New("history unavailable")}
		}, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			p, c := cleanupPlan(t)
			completeCleanup(t, p, c)
			test.change(c.info[cleanupWorkflowID(p.WorkerDeploymentName, "build-b")], c, p)
			r, _ := newTestReconciler(nil)
			err := r.planGroupRetirements(context.Background(), c, p)
			assert.Equal(t, test.wantError, err != nil)
			assert.Empty(t, p.DeleteDeployments)
			assert.Empty(t, p.MarkRevisionsForDeletion)
			assert.Empty(t, c.started)
		})
	}
}

func TestGroupedCleanupPodReadFailureRetainsProof(t *testing.T) {
	p, c := cleanupPlan(t)
	r, _ := newTestReconcilerWithInterceptors(nil, interceptor.Funcs{List: func(ctx context.Context, reader client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if _, pods := list.(*corev1.PodList); pods {
			return errors.New("pod list denied")
		}
		return reader.List(ctx, list, opts...)
	}})
	require.ErrorContains(t, r.planGroupRetirements(context.Background(), c, p), "pod list denied")
	assert.Empty(t, c.started)
	assert.Empty(t, p.DeleteDeployments)
	assert.Empty(t, p.MarkRevisionsForDeletion)
}

func TestGroupedCleanupUnavailableManagerCannotPruneTemporalOrSavedBuild(t *testing.T) {
	for _, failure := range []string{"describe unavailable", "start unavailable", "running", "failed", "success"} {
		t.Run(failure, func(t *testing.T) {
			p, c := cleanupPlan(t)
			group := p.groupRetirements[0]
			before := k8s.HashString(string(group.saved.Object.Data.Raw))
			objects := []client.Object{group.saved.Object}
			for _, member := range group.actual {
				objects = append(objects, member)
			}
			r, _ := newTestReconciler(objects)
			w := groupedWorker("build-c")
			w.Status.DeprecatedVersions = []*v1alpha1.DeprecatedWorkerDeploymentVersion{{BaseWorkerDeploymentVersion: v1alpha1.BaseWorkerDeploymentVersion{BuildID: group.buildID, Status: v1alpha1.VersionStatusDrained}}}
			handle := newPruneStubHandle(nil)
			c.Client = newStubTemporalClientWithHandle(handle)
			id := cleanupWorkflowID(p.WorkerDeploymentName, group.buildID)
			switch failure {
			case "describe unavailable":
				c.describeErrors = map[string]error{id: errors.New("manager unavailable")}
			case "start unavailable":
				c.startErr = errors.New("manager unavailable")
			default:
				completeCleanup(t, p, c)
				if failure == "running" {
					c.info[id].Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
				}
				if failure == "failed" {
					c.info[id].Status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
				}
			}
			require.NoError(t, r.executePlan(context.Background(), logr.Discard(), w, c, p))
			var saved appsv1.ControllerRevision
			require.NoError(t, r.Get(context.Background(), client.ObjectKeyFromObject(group.saved.Object), &saved))
			assert.Equal(t, before, k8s.HashString(string(saved.Data.Raw)))
			if failure == "success" {
				assert.Equal(t, []string{group.buildID}, handle.deletedVersions)
				assert.Equal(t, "true", saved.Annotations[k8s.GroupDeletionAnnotation])
			} else {
				assert.Empty(t, handle.deletedVersions)
				assert.Empty(t, saved.Annotations[k8s.GroupDeletionAnnotation])
			}
			for _, member := range group.actual {
				var observed appsv1.Deployment
				err := r.Get(context.Background(), client.ObjectKeyFromObject(member), &observed)
				if failure == "success" {
					assert.Error(t, err)
				} else {
					require.NoError(t, err)
					assert.Zero(t, *observed.Spec.Replicas)
				}
			}
		})
	}
}

func TestGroupedOldSavedBuildDoesNotInheritCleanup(t *testing.T) {
	r, _ := newTestReconciler(nil)
	old := groupedWorker("build-a")
	saved, members := savedGroup(t, r, old)
	current := groupedWorker("build-b")
	current.Spec.GroupCleanupWorkflow = &v1alpha1.GroupCleanupWorkflowConfig{WorkflowType: "CleanupBuild", TaskQueue: "new-management"}
	ts := registeredGroup("build-a")
	ts.CurrentBuildID = "build-b"
	p := groupPlan()
	require.NoError(t, r.planGroupedDeployments(current, v1alpha1.ConnectionSpec{}, ts, groupState("build-a", saved, members), p, false))
	require.Len(t, p.groupRetirements, 1)
	assert.Nil(t, p.groupRetirements[0].saved.Spec.GroupCleanupWorkflow)
	assert.NotContains(t, string(saved.Object.Data.Raw), "groupCleanupWorkflow")
	// A saved build without the optional hook still follows its previous deletion path.
	group := &p.groupRetirements[0]
	group.version = &temporal.VersionInfo{BuildID: "build-a", Status: v1alpha1.VersionStatusDrained}
	drained := time.Now().Add(-time.Hour)
	group.version.DrainedSince = &drained
	for _, member := range group.actual {
		zero := int32(0)
		member.Spec.Replicas = &zero
	}
	c := approvedRetirements(t, p, nil)
	require.NoError(t, r.planGroupRetirements(context.Background(), c, p))
	assert.Empty(t, c.started)
	assert.Len(t, p.DeleteDeployments, 2)
}

func TestGroupedCleanupIgnoresPodsOutsideSavedMemberSelectors(t *testing.T) {
	p, c := cleanupPlan(t)
	member := p.groupRetirements[0].desired["activities"]
	podLabels := make(map[string]string)
	for key, value := range member.Spec.Selector.MatchLabels {
		podLabels[key] = value
	}
	podLabels[k8s.BuildIDLabel] = "another-build"
	r, _ := newTestReconciler([]client.Object{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "other-build-pod", Namespace: member.Namespace, Labels: podLabels}}})
	require.NoError(t, r.planGroupRetirements(context.Background(), c, p))
	assert.Len(t, c.started, 1)
	assert.Empty(t, p.DeleteDeployments)
}

func TestGroupedCleanupNormalScaleStopCompleteAndPruneSequence(t *testing.T) {
	ctx := context.Background()
	setup, _ := newTestReconciler(nil)
	old := groupedWorker("build-a")
	old.Spec.GroupCleanupWorkflow = &v1alpha1.GroupCleanupWorkflowConfig{WorkflowType: "CleanupBuild", TaskQueue: "management"}
	saved, members := savedGroup(t, setup, old)
	objects := []client.Object{saved.Object}
	for _, member := range members {
		objects = append(objects, member)
	}
	// A scale request changes replicas while the API server preserves the
	// Deployment's labels and owner. Load the complete object before updating
	// because the fake scale implementation stores its partial input object.
	r, _ := newTestReconcilerWithInterceptors(objects, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, object client.Object, opts ...client.SubResourceUpdateOption) error {
		if name != "scale" {
			return c.SubResource(name).Update(ctx, object, opts...)
		}
		options := (&client.SubResourceUpdateOptions{}).ApplyOptions(opts)
		scale, ok := options.SubResourceBody.(*autoscalingv1.Scale)
		require.True(t, ok)
		var deployment appsv1.Deployment
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), &deployment); err != nil {
			return err
		}
		deployment.Spec.Replicas = &scale.Spec.Replicas
		return c.Update(ctx, &deployment)
	}})
	current := groupedWorker("build-b")
	ts := registeredGroup("build-a")
	ts.CurrentBuildID = "build-b"
	drained := time.Now().Add(-time.Hour)
	ts.Versions["build-a"].Status = v1alpha1.VersionStatusDrained
	ts.Versions["build-a"].DrainedSince = &drained
	current.Status.DeprecatedVersions = []*v1alpha1.DeprecatedWorkerDeploymentVersion{{BaseWorkerDeploymentVersion: v1alpha1.BaseWorkerDeploymentVersion{BuildID: "build-a", Status: v1alpha1.VersionStatusDrained}}}
	handle := newPruneStubHandle(nil)
	var tc *retirementClient
	for step := 0; step < 4; step++ {
		state, err := k8s.GetDeploymentState(ctx, r.Client, current.Namespace, current.Name, current.UID)
		require.NoError(t, err)
		p := groupPlan()
		p.WorkerDeploymentName = k8s.ComputeWorkerDeploymentName(current)
		p.TemporalNamespace = current.Spec.WorkerOptions.TemporalNamespace
		require.NoError(t, r.planGroupedDeployments(current, v1alpha1.ConnectionSpec{HostPort: "test:7233"}, ts, state, p, false))
		if step == 0 {
			tc = approvedRetirements(t, p, newStubTemporalClientWithHandle(handle))
		}
		if step == 2 {
			completeCleanup(t, p, tc)
		}
		require.NoError(t, r.executePlan(ctx, logr.Discard(), current, tc, p), "step %d", step)
		switch step {
		case 0:
			assert.Empty(t, tc.started)
			assert.Empty(t, handle.deletedVersions)
			for _, member := range members {
				var observed appsv1.Deployment
				require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(member), &observed))
				assert.Zero(t, *observed.Spec.Replicas)
			}
		case 1:
			require.Len(t, tc.started, 1)
			assert.Empty(t, handle.deletedVersions)
			assert.Empty(t, p.DeleteDeployments)
		case 2:
			assert.Equal(t, []string{"build-a"}, handle.deletedVersions)
			var retained appsv1.ControllerRevision
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(saved.Object), &retained))
			assert.Equal(t, "true", retained.Annotations[k8s.GroupDeletionAnnotation])
			ts.Versions = map[string]*temporal.VersionInfo{}
			current.Status.DeprecatedVersions[0].Status = v1alpha1.VersionStatusNotRegistered
		case 3:
			var removed appsv1.ControllerRevision
			assert.Error(t, r.Get(ctx, client.ObjectKeyFromObject(saved.Object), &removed))
			assert.Len(t, tc.started, 1)
			assert.Equal(t, []string{"build-a"}, handle.deletedVersions)
		}
	}
}

func TestGroupedCleanupSavedConfigurationValidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		config *v1alpha1.GroupCleanupWorkflowConfig
		valid  bool
	}{
		{name: "old saved build", valid: true},
		{name: "complete cleanup", config: &v1alpha1.GroupCleanupWorkflowConfig{WorkflowType: "CleanupBuild", TaskQueue: "management"}, valid: true},
		{name: "missing type", config: &v1alpha1.GroupCleanupWorkflowConfig{TaskQueue: "management"}},
		{name: "missing queue", config: &v1alpha1.GroupCleanupWorkflowConfig{WorkflowType: "CleanupBuild"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, _ := newTestReconciler(nil)
			w := groupedWorker("build-a")
			w.Spec.GroupCleanupWorkflow = test.config
			saved, _ := savedGroup(t, r, w)
			require.NoError(t, r.Create(context.Background(), saved.Object))
			loaded, err := k8s.GetGroupRevisions(context.Background(), r.Client, w.Namespace, w.Name, w.UID)
			if test.valid {
				require.NoError(t, err)
				assert.Equal(t, test.config, loaded["build-a"].Spec.GroupCleanupWorkflow)
			} else {
				assert.ErrorContains(t, err, "group cleanup workflow type and task queue are required")
			}
		})
	}
}
