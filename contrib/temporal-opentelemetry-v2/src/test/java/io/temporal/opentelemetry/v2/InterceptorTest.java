package io.temporal.opentelemetry.v2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.temporal.client.WorkflowFailedException;
import io.temporal.opentelemetry.v2.TestWorkflows.AsyncLambdaWorkflow;
import io.temporal.opentelemetry.v2.TestWorkflows.AsyncLambdaWorkflowImpl;
import io.temporal.opentelemetry.v2.TestWorkflows.BenignErrorWorkflow;
import io.temporal.opentelemetry.v2.TestWorkflows.BenignErrorWorkflowImpl;
import io.temporal.opentelemetry.v2.TestWorkflows.ErrorWorkflow;
import io.temporal.opentelemetry.v2.TestWorkflows.ErrorWorkflowImpl;
import io.temporal.opentelemetry.v2.TestWorkflows.SpanKindWorkflow;
import io.temporal.opentelemetry.v2.TestWorkflows.SpanKindWorkflowImpl;
import io.temporal.opentelemetry.v2.TestWorkflows.TestActivitiesImpl;
import io.temporal.testing.internal.SDKTestWorkflowRule;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;

/** Tests interceptor behavior. */
public class InterceptorTest extends OtelTestBase {
  @Rule
  public SDKTestWorkflowRule testWorkflowRule =
      newRuleBuilder(true)
          .setWorkflowTypes(
              SpanKindWorkflowImpl.class,
              BenignErrorWorkflowImpl.class,
              ErrorWorkflowImpl.class,
              AsyncLambdaWorkflowImpl.class)
          .setActivityImplementations(new TestActivitiesImpl())
          .build();

  @Test
  public void spanKind() {
    testWorkflowRule.newWorkflowStub(SpanKindWorkflow.class).run();

    Map<String, SpanKind> kinds = new HashMap<>();
    for (SpanData span : endedSpans()) {
      kinds.put(span.getName(), span.getKind());
    }
    assertEquals(SpanKind.SERVER, kinds.get("RunWorkflow:SpanKindWorkflow"));
    assertEquals(SpanKind.CLIENT, kinds.get("StartActivity:NopActivity"));
    assertEquals(SpanKind.SERVER, kinds.get("RunActivity:NopActivity"));
  }

  @Test
  public void asyncLambdaPreservesApplicationContext() {
    testWorkflowRule.newWorkflowStub(AsyncLambdaWorkflow.class).run();

    assertSpanTree(
        Arrays.asList(
            "StartWorkflow:AsyncLambdaWorkflow",
            "  RunWorkflow:AsyncLambdaWorkflow",
            "    parent",
            "      child",
            "        StartActivity:NopActivity",
            "          RunActivity:NopActivity"),
        endedSpans());
  }

  @Test
  public void benignErrorLeavesSpanStatusUnset() {
    assertThrows(
        WorkflowFailedException.class,
        () -> testWorkflowRule.newWorkflowStub(BenignErrorWorkflow.class).run());

    assertSpanTree(
        Arrays.asList("StartWorkflow:BenignErrorWorkflow", "  RunWorkflow:BenignErrorWorkflow"),
        endedSpans());
    SpanData run = requireSpanNamed(endedSpans(), "RunWorkflow:BenignErrorWorkflow");
    assertEquals(StatusCode.UNSET, run.getStatus().getStatusCode());
  }

  @Test
  public void errorSetsSpanStatusError() {
    assertThrows(
        WorkflowFailedException.class,
        () -> testWorkflowRule.newWorkflowStub(ErrorWorkflow.class).run());

    assertSpanTree(
        Arrays.asList("StartWorkflow:ErrorWorkflow", "  RunWorkflow:ErrorWorkflow"), endedSpans());
    SpanData run = requireSpanNamed(endedSpans(), "RunWorkflow:ErrorWorkflow");
    assertEquals(StatusCode.ERROR, run.getStatus().getStatusCode());
  }
}
