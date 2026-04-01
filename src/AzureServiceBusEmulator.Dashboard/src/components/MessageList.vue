<script setup lang="ts">
import type { MessageInfo } from '../types'

defineProps<{
  namespace: string
  entity: string
  entityType: 'queue' | 'topic' | null
}>()

defineModel<MessageInfo | null>('selectedMessage')
</script>

<template>
  <div class="message-list">
    <div class="placeholder">Message List</div>
  </div>
</template>

<style scoped>
.message-list { width: 380px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; }
.placeholder { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
