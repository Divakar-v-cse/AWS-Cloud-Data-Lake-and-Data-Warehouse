/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        ink: '#0b1220',
        panel: '#111a2c',
        line: '#1e2b45',
        accent: '#3ddc97',
        accent2: '#5b9cf8',
      },
    },
  },
  plugins: [],
}
