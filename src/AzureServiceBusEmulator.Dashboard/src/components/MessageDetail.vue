<script setup lang="ts">
import type { MessageInfo } from '../types'

defineProps<{ message: MessageInfo }>()
</script>

<template>
  <div class="detail">
    <div class="placeholder">Message Detail</div>
  </div>
</template>

<style scoped>
.detail { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
.placeholder { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
