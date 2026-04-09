<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{
  counts: Record<string, number>
}>()

const queues = [
  { name: 'process-payment',    stateKey: 'PaymentPending',     color: '#f59e0b' },
  { name: 'reserve-inventory',  stateKey: 'InventoryReserving', color: '#c98a2e' },
  { name: 'pick-order',         stateKey: 'Picking',            color: '#7b5ea7' },
  { name: 'logistics-dispatch', stateKey: 'Shipping',           color: '#2a7ab5' },
  { name: 'generate-invoice',   stateKey: 'Delivered',          color: '#2d8f52' },
]

const maxCount = computed(() => Math.max(1, ...queues.map(q => props.counts[q.stateKey] || 0)))
</script>

<template>
  <div class="queue-list">
    <div v-for="q in queues" :key="q.name" class="queue-item">
      <span class="queue-name">{{ q.name }}</span>
      <div class="queue-bar-track">
        <div
          class="queue-bar-fill"
          :style="{
            width: `${((counts[q.stateKey] || 0) / maxCount) * 100}%`,
            background: q.color,
          }"
        ></div>
      </div>
      <span class="queue-count">{{ counts[q.stateKey] || 0 }}</span>
    </div>
  </div>
</template>
