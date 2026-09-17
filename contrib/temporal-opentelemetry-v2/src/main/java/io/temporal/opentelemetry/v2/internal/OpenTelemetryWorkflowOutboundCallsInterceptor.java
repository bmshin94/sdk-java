package io.temporal.opentelemetry.v2.internal;

import static io.temporal.opentelemetry.v2.internal.TagKeys.*;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptor;
import io.temporal.common.interceptors.WorkflowOutboundCallsInterceptorBase;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInfo;

public class OpenTelemetryWorkflowOutboundCallsInterceptor
    extends WorkflowOutboundCallsInterceptorBase {

  private final InterceptorTracer tracer;

  public OpenTelemetryWorkflowOutboundCallsInterceptor(
      InterceptorTracer tracer, WorkflowOutboundCallsInterceptor next) {
    super(next);
    this.tracer = tracer;
  }

  @Override
  public <R> ActivityOutput<R> executeActivity(ActivityInput<R> input) {
    return tracer.traceOutbound(
        "StartActivity",
        input.getActivityName(),
        workflowTags(),
        input.getHeader(),
        () -> super.executeActivity(input));
  }

  @Override
  public <R> LocalActivityOutput<R> executeLocalActivity(LocalActivityInput<R> input) {
    return tracer.traceOutbound(
        "StartActivity",
        input.getActivityName(),
        workflowTags(),
        input.getHeader(),
        () -> super.executeLocalActivity(input));
  }

  @Override
  public <R> ChildWorkflowOutput<R> executeChildWorkflow(ChildWorkflowInput<R> input) {
    return tracer.traceOutbound(
        "StartChildWorkflow",
        input.getWorkflowType(),
        workflowTags(),
        input.getHeader(),
        () -> super.executeChildWorkflow(input));
  }

  @Override
  public <R> ExecuteNexusOperationOutput<R> executeNexusOperation(
      ExecuteNexusOperationInput<R> input) {
    return tracer.traceNexusOutbound(
        "StartNexusOperation",
        input.getService() + "/" + input.getOperation(),
        nexusTags(input),
        input.getHeaders(),
        () -> super.executeNexusOperation(input));
  }

  @Override
  public SignalExternalOutput signalExternalWorkflow(SignalExternalInput input) {
    return tracer.traceOutbound(
        "SignalExternalWorkflow",
        input.getSignalName(),
        workflowTags(),
        input.getHeader(),
        () -> super.signalExternalWorkflow(input));
  }

  @Override
  public CancelWorkflowOutput cancelWorkflow(CancelWorkflowInput input) {
    return tracer.traceOutbound(
        "CancelWorkflow", "", workflowTags(), () -> super.cancelWorkflow(input));
  }

  @Override
  public void continueAsNew(ContinueAsNewInput input) {
    tracer.traceOutbound(
        "ContinueAsNew",
        Workflow.getInfo().getWorkflowType(),
        workflowTags(),
        input.getHeader(),
        () -> super.continueAsNew(input));
  }

  private static Attributes workflowTags() {
    WorkflowInfo info = Workflow.getInfo();
    return Attributes.of(WORKFLOW_ID, info.getWorkflowId(), RUN_ID, info.getRunId());
  }

  private static Attributes nexusTags(ExecuteNexusOperationInput<?> input) {
    AttributesBuilder tags =
        workflowTags().toBuilder()
            .put(NEXUS_SERVICE, input.getService())
            .put(NEXUS_OPERATION, input.getOperation())
            .put(NEXUS_ENDPOINT, input.getEndpoint());

    return tags.build();
  }
}
