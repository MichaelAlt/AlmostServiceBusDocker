<script setup lang="ts">
import { ref } from 'vue'

defineProps<{
  connected: boolean
}>()

const active = ref('Overview')
const items = ['Overview', 'Pipeline', 'Queues', 'Sagas', 'Scenarios']
</script>

<template>
  <nav class="sidebar">
    <div class="sidebar-logo">OrderFlow</div>

    <div class="sidebar-nav">
      <div
        v-for="item in items"
        :key="item"
        class="sidebar-item"
        :class="{ active: active === item }"
        @click="active = item"
      >
        {{ item }}
      </div>
    </div>

    <div class="sidebar-footer">
      AlmostServiceBus Demo v0.1.0<br>
      MassTransit<br>
      <span :style="{ color: connected ? '#2d8f52' : '#9ca3af' }">
        {{ connected ? '● Live' : '○ Offline' }}
      </span>
    </div>
  </nav>
</template>
