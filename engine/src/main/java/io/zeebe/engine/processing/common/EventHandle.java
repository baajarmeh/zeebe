/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.common;

import io.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.processing.deployment.model.element.ExecutableStartEvent;
import io.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.zeebe.engine.state.KeyGenerator;
import io.zeebe.engine.state.instance.ElementInstance;
import io.zeebe.engine.state.mutable.MutableEventScopeInstanceState;
import io.zeebe.protocol.impl.record.value.workflowinstance.WorkflowInstanceRecord;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import org.agrona.DirectBuffer;

public final class EventHandle {

  private final WorkflowInstanceRecord eventOccurredRecord = new WorkflowInstanceRecord();
  private final KeyGenerator keyGenerator;
  private final MutableEventScopeInstanceState eventScopeInstanceState;

  public EventHandle(
      final KeyGenerator keyGenerator,
      final MutableEventScopeInstanceState eventScopeInstanceState) {
    this.keyGenerator = keyGenerator;
    this.eventScopeInstanceState = eventScopeInstanceState;
  }

  public boolean triggerEvent(
      final TypedStreamWriter streamWriter,
      final ElementInstance eventScopeInstance,
      final ExecutableFlowElement catchEvent,
      final DirectBuffer variables) {

    if (eventScopeInstance == null || !eventScopeInstance.isActive()) {
      // discard the event if the element instance is left
      return false;
    }

    final var newElementInstanceKey = keyGenerator.nextKey();
    final var triggered =
        eventScopeInstanceState.triggerEvent(
            eventScopeInstance.getKey(), newElementInstanceKey, catchEvent.getId(), variables);

    if (triggered) {

      final long eventOccurredKey;

      if (isEventSubprocess(catchEvent)) {

        eventOccurredKey = keyGenerator.nextKey();
        eventOccurredRecord.wrap(eventScopeInstance.getValue());
        eventOccurredRecord
            .setElementId(catchEvent.getId())
            .setBpmnElementType(BpmnElementType.START_EVENT)
            .setFlowScopeKey(eventScopeInstance.getKey());

      } else {
        eventOccurredKey = eventScopeInstance.getKey();
        eventOccurredRecord.wrap(eventScopeInstance.getValue());
      }

      streamWriter.appendFollowUpEvent(
          eventOccurredKey, WorkflowInstanceIntent.EVENT_OCCURRED, eventOccurredRecord);
    }

    return triggered;
  }

  public long triggerStartEvent(
      final TypedStreamWriter streamWriter,
      final long workflowKey,
      final DirectBuffer elementId,
      final DirectBuffer variables) {

    final var newElementInstanceKey = keyGenerator.nextKey();
    final var triggered =
        eventScopeInstanceState.triggerEvent(
            workflowKey, newElementInstanceKey, elementId, variables);

    if (triggered) {
      final var workflowInstanceKey = keyGenerator.nextKey();
      activateStartEvent(streamWriter, workflowKey, workflowInstanceKey, elementId);
      return workflowInstanceKey;

    } else {
      return -1L;
    }
  }

  public void activateStartEvent(
      final TypedStreamWriter streamWriter,
      final long workflowKey,
      final long workflowInstanceKey,
      final DirectBuffer elementId) {

    final var eventOccurredKey = keyGenerator.nextKey();

    eventOccurredRecord
        .setBpmnElementType(BpmnElementType.START_EVENT)
        .setWorkflowKey(workflowKey)
        .setWorkflowInstanceKey(workflowInstanceKey)
        .setElementId(elementId);

    // TODO (saig0): create the workflow instance by writing an ACTIVATE command (#6184)
    streamWriter.appendFollowUpEvent(
        eventOccurredKey, WorkflowInstanceIntent.EVENT_OCCURRED, eventOccurredRecord);
  }

  private boolean isEventSubprocess(final ExecutableFlowElement catchEvent) {
    return catchEvent instanceof ExecutableStartEvent
        && ((ExecutableStartEvent) catchEvent).getEventSubProcess() != null;
  }
}
