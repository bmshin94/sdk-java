package io.temporal.opentelemetry.v2.internal;

import io.opentelemetry.api.common.Attributes;
import io.temporal.client.schedules.ScheduleActionStartWorkflow;
import io.temporal.common.interceptors.ScheduleClientCallsInterceptor;
import io.temporal.common.interceptors.ScheduleClientCallsInterceptorBase;

public class OpenTelemetryScheduleClientCallsInterceptor
    extends ScheduleClientCallsInterceptorBase {
  private final InterceptorTracer tracer;

  public OpenTelemetryScheduleClientCallsInterceptor(
      InterceptorTracer tracer, ScheduleClientCallsInterceptor next) {
    super(next);
    this.tracer = tracer;
  }

  @Override
  public void createSchedule(CreateScheduleInput input) {
    if (!(input.getSchedule().getAction() instanceof ScheduleActionStartWorkflow)) {
      super.createSchedule(input);
      return;
    }

    tracer.traceOutbound(
        "CreateSchedule",
        input.getId(),
        Attributes.empty(),
        ((ScheduleActionStartWorkflow) input.getSchedule().getAction()).getHeader(),
        () -> super.createSchedule(input));
  }
}
