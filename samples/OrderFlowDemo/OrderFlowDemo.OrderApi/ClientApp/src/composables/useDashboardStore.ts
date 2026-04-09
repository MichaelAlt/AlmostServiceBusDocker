import { reactive, computed } from 'vue'
import type { DashboardEvent } from './useSse'

export interface FeedItem {
  id: string
  type: string
  orderId?: string
  state?: string
  warehouse?: string
  failureReason?: string
  timestamp: string
}

const state = reactive({
  pipelineCounts: {} as Record<string, number>,
  warehouseDepths: {} as Record<string, number>,
  totalOrders: 0,
  completedOrders: 0,
  failedOrders: 0,
  feedItems: [] as FeedItem[],
  throughputHistory: [] as { time: number; count: number }[],
  recentEventCount: 0,
})

const TERMINAL_STATES = ['Invoiced', 'Final']
const FAILURE_STATES = ['PaymentFailed', 'BackOrdered']
const MAX_FEED_ITEMS = 50
const THROUGHPUT_WINDOW_MS = 60_000

let eventCounter = 0

export function useDashboardStore() {
  function processEvent(event: DashboardEvent) {
    if (event.type !== 'saga-transition') return

    const toState = event.toState
    if (!toState) return

    // Update pipeline counts
    if (event.fromState && state.pipelineCounts[event.fromState] > 0) {
      state.pipelineCounts[event.fromState]--
    }
    state.pipelineCounts[toState] = (state.pipelineCounts[toState] || 0) + 1

    // Track totals
    if (!event.fromState) {
      state.totalOrders++
    }
    if (TERMINAL_STATES.includes(toState)) {
      state.completedOrders++
    }
    if (FAILURE_STATES.includes(toState)) {
      state.failedOrders++
    }

    // Warehouse depths — order enters warehouse at Picking, leaves at Shipping
    if (event.warehouse) {
      if (toState === 'Picking') {
        state.warehouseDepths[event.warehouse] = (state.warehouseDepths[event.warehouse] || 0) + 1
      } else if (toState === 'Shipping' || FAILURE_STATES.includes(toState)) {
        state.warehouseDepths[event.warehouse] = Math.max(
          0, (state.warehouseDepths[event.warehouse] || 0) - 1)
      }
    }

    // Feed
    state.feedItems.unshift({
      id: `${++eventCounter}`,
      type: event.type,
      orderId: event.orderId,
      state: toState,
      warehouse: event.warehouse ?? undefined,
      failureReason: event.failureReason ?? undefined,
      timestamp: event.timestamp,
    })
    if (state.feedItems.length > MAX_FEED_ITEMS) {
      state.feedItems.length = MAX_FEED_ITEMS
    }

    // Throughput tracking
    const now = Date.now()
    state.throughputHistory.push({ time: now, count: 1 })
    state.throughputHistory = state.throughputHistory.filter(
      e => now - e.time < THROUGHPUT_WINDOW_MS)
    state.recentEventCount = state.throughputHistory.length
  }

  const inFlight = computed(() =>
    state.totalOrders - state.completedOrders - state.failedOrders)

  const throughputPerSecond = computed(() =>
    state.throughputHistory.length > 0
      ? Math.round(state.recentEventCount / (THROUGHPUT_WINDOW_MS / 1000) * 10) / 10
      : 0)

  return {
    state,
    processEvent,
    inFlight,
    throughputPerSecond,
  }
}
