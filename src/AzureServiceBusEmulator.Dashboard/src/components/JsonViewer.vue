<script setup lang="ts">
import { computed } from 'vue'

const props = defineProps<{ json: string }>()

const formatted = computed(() => {
  try {
    return JSON.stringify(JSON.parse(props.json), null, 2)
  } catch {
    return props.json
  }
})
</script>

<template>
  <pre class="json-viewer">{{ formatted }}</pre>
</template>

<style scoped>
.json-viewer {
  margin: 0;
  font-family: 'Cascadia Code', 'Fira Code', monospace;
  font-size: 11px;
  line-height: 1.6;
  color: var(--text);
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
