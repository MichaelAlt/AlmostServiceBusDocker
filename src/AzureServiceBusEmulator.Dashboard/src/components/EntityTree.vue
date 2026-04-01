<script setup lang="ts">
defineModel<string>('namespace', { required: true })
defineModel<string | null>('entity', { required: true })
defineModel<'queue' | 'topic' | null>('entityType', { required: true })
defineEmits<{ select: [] }>()
</script>

<template>
  <div class="sidebar">
    <div class="placeholder">Entity Tree</div>
  </div>
</template>

<style scoped>
.sidebar { width: 300px; border-right: 1px solid var(--border); display: flex; flex-direction: column; flex-shrink: 0; background: var(--bg-base); }
.placeholder { padding: 20px; text-align: center; color: var(--text-muted); }
</style>
