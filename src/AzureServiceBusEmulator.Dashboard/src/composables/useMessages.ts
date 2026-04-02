import { ref } from 'vue'
import type { MessageInfo, MessageEvent } from '../types'
import { api } from '../api/client'
import { connectSse } from '../api/sse'

export function useMessages(ns: () => string, entity: () => string | null, entityType: () => 'queue' | 'topic' | null) {
  const messages = ref<MessageInfo[]>([])
  const connected = ref(false)
  let source: EventSource | null = null

  async function refresh() {
    const e = entity()
    const t = entityType()
    if (!e) { messages.value = []; return }
    try {
      messages.value = t === 'topic'
        ? await api.getTopicMessages(ns(), e)
        : await api.getQueueMessages(ns(), e)
    } catch { /* ignore */ }
  }

  function startSse() {
    stopSse()
    const e = entity()
    if (!e) return

    source = connectSse(ns(), e, (evt: MessageEvent) => {
      if (evt.type === 'Enqueued') {
        messages.value.unshift({
          messageId: evt.messageId,
          sequenceNumber: evt.sequenceNumber,
          contentType: evt.contentType,
          correlationId: null,
          deliveryCount: 0,
          enqueuedTimeUtc: evt.timestamp,
          subject: null,
          applicationProperties: null,
          bodyText: evt.bodyPreview,
          scalarProperties: evt.scalarProperties,
        })
        if (messages.value.length > 100) messages.value.pop()
      }
    })

    source.onopen = () => { connected.value = true }
    source.onerror = () => { connected.value = false }
  }

  function stopSse() {
    source?.close()
    source = null
    connected.value = false
  }

  return { messages, connected, refresh, startSse, stopSse }
}
