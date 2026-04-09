<script setup lang="ts">
// 4 UK warehouses with coloured order blocks and opacity gradient (front of queue = full opacity)
const WAREHOUSES = [
  { id: 'London-East', label: 'London East' },
  { id: 'Manchester',  label: 'Manchester'  },
  { id: 'Birmingham',  label: 'Birmingham'  },
  { id: 'Edinburgh',   label: 'Edinburgh'   },
]

// Order blocks use a teal-to-amber colour ramp — each block gets a slightly different hue
// Opacity fades from full (front) to 0.3 (back) to suggest FIFO depth
const BLOCK_COLORS = ['#068d9d', '#2a7ab5', '#7b5ea7', '#c98a2e', '#2d8f52', '#f59e0b', '#c11f1f', '#f97316']

const props = defineProps<{
  depths: Record<string, number>
}>()

function blocks(warehouseId: string): { color: string; opacity: number }[] {
  const depth = props.depths[warehouseId] || 0
  const visible = Math.min(depth, 24) // cap display at 24 blocks
  return Array.from({ length: visible }, (_, i) => ({
    color: BLOCK_COLORS[i % BLOCK_COLORS.length],
    // front of queue (index 0) = full opacity; back = 0.35 minimum
    opacity: Math.max(0.35, 1 - (i / Math.max(visible - 1, 1)) * 0.65),
  }))
}
</script>

<template>
  <div class="warehouse-grid">
    <div v-for="wh in WAREHOUSES" :key="wh.id" class="warehouse-lane">
      <div class="warehouse-lane-title">{{ wh.label }}</div>
      <div class="warehouse-blocks">
        <div
          v-for="(block, i) in blocks(wh.id)"
          :key="i"
          class="order-block"
          :style="{ background: block.color, opacity: block.opacity }"
        ></div>
        <span v-if="blocks(wh.id).length === 0" style="font-size: 11px; color: #9ca3af;">empty</span>
      </div>
      <div class="warehouse-depth-label">{{ depths[wh.id] || 0 }} orders in queue</div>
    </div>
  </div>
</template>
