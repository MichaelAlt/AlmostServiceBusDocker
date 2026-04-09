import { shallowRef } from 'vue'
import type { InjectionKey } from 'vue'
import type { MessageEvent } from '../types'

type EventHandler = (evt: MessageEvent) => void

export interface NamespaceSse {
  connected: ReturnType<typeof shallowRef<boolean>>
  subscribe: (handler: EventHandler) => () => void
  connect: (namespace: string) => void
  disconnect: () => void
}

export const sseKey: InjectionKey<NamespaceSse> = Symbol('namespace-sse')

export function useNamespaceSse(): NamespaceSse {
  const connected = shallowRef(false)
  let source: EventSource | null = null
  const handlers = new Set<EventHandler>()

  function connect(namespace: string) {
    disconnect()
    if (!namespace) return

    const params = new URLSearchParams({ ns: namespace })
    source = new EventSource(`/api/dashboard/events?${params}`)

    source.onmessage = (e) => {
      try {
        const parsed: MessageEvent = JSON.parse(e.data)
        for (const handler of handlers) handler(parsed)
      } catch { /* ignore parse errors */ }
    }
    source.onopen = () => { connected.value = true }
    source.onerror = () => { connected.value = false }
  }

  function disconnect() {
    source?.close()
    source = null
    connected.value = false
  }

  function subscribe(handler: EventHandler): () => void {
    handlers.add(handler)
    return () => handlers.delete(handler)
  }

  return { connected, connect, disconnect, subscribe }
}
