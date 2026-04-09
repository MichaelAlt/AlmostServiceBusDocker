<script setup lang="ts">
import { ref, onMounted } from 'vue'

interface Scenario {
  Name: string
  Description: string
  OrdersPerSecond: number
}

const scenarios = ref<Scenario[]>([])
const activeScenario = ref<string | null>(null)

onMounted(async () => {
  try {
    const res = await fetch('/api/scenarios')
    scenarios.value = await res.json()
    const activeRes = await fetch('/api/scenarios/active')
    const data = await activeRes.json()
    activeScenario.value = data.scenario ?? null
  } catch {
    // ignore — dev mode without backend
  }
})

async function startScenario(name: string) {
  await fetch(`/api/scenarios/${name}/start`, { method: 'POST' })
  activeScenario.value = name
}

async function stopScenario() {
  await fetch('/api/scenarios/stop', { method: 'POST' })
  activeScenario.value = null
}
</script>

<template>
  <!-- Inline pill row — rendered inside the top-bar, no card wrapper -->
  <div class="scenario-bar">
    <button
      v-for="s in scenarios"
      :key="s.Name"
      class="scenario-pill"
      :class="{ active: activeScenario === s.Name }"
      :title="s.Description"
      @click="startScenario(s.Name)"
    >
      {{ s.Name }}
    </button>
    <button
      v-if="activeScenario"
      class="scenario-pill stop"
      @click="stopScenario"
    >
      Stop
    </button>
    <span v-if="scenarios.length === 0" style="font-size: 12px; color: #9ca3af;">
      No scenarios loaded
    </span>
  </div>
</template>
