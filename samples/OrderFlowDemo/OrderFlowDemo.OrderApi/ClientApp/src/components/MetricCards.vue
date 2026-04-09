<script setup lang="ts">
import type { ComputedRef } from 'vue'

defineProps<{
  total: number
  completed: number
  failed: number
  inFlight: ComputedRef<number> | number
  throughput: ComputedRef<number> | number
}>()
</script>

<template>
  <div class="metric-grid">
    <!-- Total Orders: red background -->
    <div class="metric-card bg-red">
      <div class="metric-label">Total Orders</div>
      <div class="metric-value">{{ total }}</div>
    </div>

    <!-- Completed: white background, green value -->
    <div class="metric-card bg-white">
      <div class="metric-label">Completed</div>
      <div class="metric-value val-green">{{ completed }}</div>
    </div>

    <!-- In Flight: teal background -->
    <div class="metric-card bg-teal">
      <div class="metric-label">In Flight</div>
      <div class="metric-value">{{ inFlight }}</div>
    </div>

    <!-- Throughput: white background -->
    <div class="metric-card bg-white">
      <div class="metric-label">Throughput /s</div>
      <div class="metric-value">{{ throughput }}</div>
    </div>

    <!-- Dead Letters / Failed: white background, red value -->
    <div class="metric-card bg-white">
      <div class="metric-label">Dead Letters</div>
      <div class="metric-value val-red">{{ failed }}</div>
    </div>
  </div>
</template>
