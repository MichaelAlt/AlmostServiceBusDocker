<script setup lang="ts">
import type { NamespaceInfo } from '../types'

defineProps<{ namespaces: NamespaceInfo[]; selected: string }>()
const emit = defineEmits<{ 'update:selected': [value: string] }>()
</script>

<template>
  <div class="tabs">
    <div
      v-for="ns in namespaces"
      :key="ns.name"
      class="tab"
      :class="{ active: ns.name === selected }"
      @click="emit('update:selected', ns.name)"
    >
      {{ ns.name }}
    </div>
  </div>
</template>

<style scoped>
.tabs { display: flex; border-bottom: 1px solid var(--border); background: var(--bg-mantle); }
.tab { padding: 8px 14px; font-size: 11px; color: var(--text-muted); cursor: pointer; }
.tab.active { background: var(--bg-base); border-bottom: 2px solid var(--blue); color: var(--blue); font-weight: 600; }
</style>
