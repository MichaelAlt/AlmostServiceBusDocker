<script setup lang="ts">
import { ref, computed } from 'vue'
import type { FeedItem } from '../composables/useDashboardStore'

const props = defineProps<{
  items: FeedItem[]
}>()

const filter = ref<'all' | 'completed' | 'failed' | 'sessions'>('all')

const FAILURE_STATES  = ['PaymentFailed', 'BackOrdered']
const TERMINAL_STATES = ['Invoiced', 'Final']

const filtered = computed(() => {
  if (filter.value === 'all')       return props.items
  if (filter.value === 'completed') return props.items.filter(i => TERMINAL_STATES.includes(i.state ?? ''))
  if (filter.value === 'failed')    return props.items.filter(i => FAILURE_STATES.includes(i.state ?? ''))
  if (filter.value === 'sessions')  return props.items.filter(i => !!i.warehouse)
  return props.items
})

// Dot class — maps state to a CSS modifier
function dotClass(state?: string): string {
  if (!state) return ''
  if (FAILURE_STATES.includes(state))  return 'failed'
  if (TERMINAL_STATES.includes(state)) return 'completed'
  return ''
}

// Stage-specific text colour for inline state label
const STATE_COLORS: Record<string, string> = {
  PaymentPending:     '#f59e0b',
  InventoryReserving: '#c98a2e',
  Picking:            '#7b5ea7',
  Shipping:           '#2a7ab5',
  Shipped:            '#14b8a6',
  Delivered:          '#2d8f52',
  Invoiced:           '#6b7280',
  PaymentFailed:      '#c11f1f',
  BackOrdered:        '#f97316',
}

function stateColor(state?: string): string {
  return state ? (STATE_COLORS[state] ?? '#363537') : '#363537'
}

const FILTERS = ['all', 'completed', 'failed', 'sessions'] as const

function formatTime(ts: string): string {
  try { return new Date(ts).toLocaleTimeString() }
  catch { return ts }
}
</script>

<template>
  <div>
    <!-- Filter pills -->
    <div class="feed-filters">
      <button
        v-for="f in FILTERS"
        :key="f"
        class="feed-filter-pill"
        :class="{ active: filter === f }"
        @click="filter = f"
      >
        {{ f }}
      </button>
    </div>

    <!-- Event list -->
    <div class="feed-list">
      <div v-for="item in filtered" :key="item.id" class="feed-item">
        <span class="feed-dot" :class="dotClass(item.state)"></span>
        <span class="feed-text">
          <strong :style="{ color: stateColor(item.state) }">{{ item.state }}</strong>
          <span v-if="item.orderId" style="color: #9ca3af;"> · {{ item.orderId.slice(0, 8) }}</span>
          <span v-if="item.warehouse"> · {{ item.warehouse }}</span>
          <span v-if="item.failureReason" style="color: #c11f1f;"> — {{ item.failureReason }}</span>
        </span>
        <span class="feed-time">{{ formatTime(item.timestamp) }}</span>
      </div>

      <div v-if="filtered.length === 0" style="color: #9ca3af; font-size: 12px; padding: 10px 0;">
        No events yet. Start a scenario to see live data.
      </div>
    </div>
  </div>
</template>
