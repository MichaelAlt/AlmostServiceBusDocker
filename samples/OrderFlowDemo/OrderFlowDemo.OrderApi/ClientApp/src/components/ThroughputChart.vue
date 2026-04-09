<script setup lang="ts">
import { computed } from 'vue'
import { Line } from 'vue-chartjs'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
} from 'chart.js'

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip)

const props = defineProps<{
  history: { time: number; count: number }[]
}>()

const chartData = computed(() => {
  const now = Date.now()
  const buckets: number[] = new Array(60).fill(0)
  for (const entry of props.history) {
    const secondsAgo = Math.floor((now - entry.time) / 1000)
    if (secondsAgo < 60) {
      buckets[59 - secondsAgo]++
    }
  }
  const labels = buckets.map((_, i) => i === 59 ? 'now' : `${59 - i}s`)
  return {
    labels,
    datasets: [{
      label: 'Events/sec',
      data: buckets,
      fill: true,
      borderColor: '#068d9d',
      backgroundColor: 'rgba(6,141,157,0.2)',
      tension: 0.3,
      pointRadius: 0,
    }],
  }
})

const options = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: { legend: { display: false } },
  scales: {
    x: { display: false },
    y: { beginAtZero: true, ticks: { precision: 0 } },
  },
}
</script>

<template>
  <div class="chart-container">
    <Line :data="chartData" :options="options" />
  </div>
</template>
