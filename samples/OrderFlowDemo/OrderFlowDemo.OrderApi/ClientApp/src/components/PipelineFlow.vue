<script setup lang="ts">
defineProps<{
  counts: Record<string, number>
}>()

// Mockup stage colors: amber (payment), inventory (#c98a2e), picking (purple), shipping (blue), delivered (green)
const stages = [
  { key: 'PaymentPending',    label: 'Payment',   color: '#f59e0b' },
  { key: 'InventoryReserving', label: 'Inventory', color: '#c98a2e' },
  { key: 'Picking',           label: 'Picking',   color: '#7b5ea7' },
  { key: 'Shipping',          label: 'Shipping',  color: '#2a7ab5' },
  { key: 'Shipped',           label: 'Shipped',   color: '#14b8a6' },
  { key: 'Delivered',         label: 'Delivered', color: '#2d8f52' },
  { key: 'Invoiced',          label: 'Invoiced',  color: '#6b7280' },
  { key: 'PaymentFailed',     label: 'Failed',    color: '#c11f1f' },
  { key: 'BackOrdered',       label: 'BackOrder', color: '#f97316' },
]
</script>

<template>
  <div class="pipeline-nodes">
    <template v-for="(stage, i) in stages" :key="stage.key">
      <div class="pipeline-node">
        <div
          class="pipeline-node-rect"
          :class="{ 'has-orders': (counts[stage.key] || 0) > 0 }"
          :style="{ background: (counts[stage.key] || 0) > 0 ? stage.color : '#d1d5db' }"
        >
          <span class="pipeline-node-rect-label">{{ stage.label }}</span>
          <span class="pipeline-node-rect-count">{{ counts[stage.key] || 0 }}</span>
        </div>
      </div>
      <div v-if="i < stages.length - 1" class="pipeline-arrow">›</div>
    </template>
  </div>
</template>
