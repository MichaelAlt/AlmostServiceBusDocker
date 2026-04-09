import { ref, onUnmounted } from 'vue'

export interface DashboardEvent {
  type: 'saga-transition' | 'message-consumed' | 'message-dead-lettered'
  orderId?: string
  fromState?: string
  toState?: string
  warehouse?: string
  customerName?: string
  products?: string[]
  amount?: number
  queueName?: string
  failureReason?: string
  timestamp: string
}

export function useSse(url: string) {
  const connected = ref(false)
  const lastEvent = ref<DashboardEvent | null>(null)
  const handlers: Array<(event: DashboardEvent) => void> = []

  const eventSource = new EventSource(url)

  eventSource.onopen = () => { connected.value = true }
  eventSource.onerror = () => { connected.value = false }

  eventSource.onmessage = (e) => {
    const event: DashboardEvent = JSON.parse(e.data)
    lastEvent.value = event
    handlers.forEach(h => h(event))
  }

  function onEvent(handler: (event: DashboardEvent) => void) {
    handlers.push(handler)
  }

  onUnmounted(() => {
    eventSource.close()
  })

  return { connected, lastEvent, onEvent }
}
