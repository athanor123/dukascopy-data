name: Historique scanner Gold'n'Eye (H1 + M15, 16 actifs)

# À placer dans le dépôt : .github/workflows/historique-scanner.yml
# Le script historique_scanner.py doit être à la racine du dépôt.
# 1er lancement : Actions → « Historique scanner Gold'n'Eye » → Run workflow (télécharge depuis le 01/01/2024).
# Ensuite : mise à jour automatique chaque soir de semaine à 22:30 UTC (seules les nouvelles journées).

on:
  workflow_dispatch:
  schedule:
    - cron: "30 22 * * 1-5"

permissions:
  contents: write

concurrency:
  group: historique-scanner
  cancel-in-progress: false

jobs:
  actif:
    runs-on: ubuntu-latest
    timeout-minutes: 300
    strategy:
      fail-fast: false
      max-parallel: 8
      matrix:
        include:
          - { ticker: XAUUSD,    base: OANDA_XAUUSD }
          - { ticker: XAGUSD,    base: OANDA_XAGUSD }
          - { ticker: WTICOUSD,  base: OANDA_WTICOUSD }
          - { ticker: EURUSD,    base: OANDA_EURUSD }
          - { ticker: GBPUSD,    base: OANDA_GBPUSD }
          - { ticker: USDJPY,    base: OANDA_USDJPY }
          - { ticker: AUDUSD,    base: OANDA_AUDUSD }
          - { ticker: USDCAD,    base: OANDA_USDCAD }
          - { ticker: USDCHF,    base: OANDA_USDCHF }
          - { ticker: GBPJPY,    base: OANDA_GBPJPY }
          - { ticker: EURJPY,    base: OANDA_EURJPY }
          - { ticker: NAS100USD, base: OANDA_NAS100USD }
          - { ticker: SPX500USD, base: OANDA_SPX500USD }
          - { ticker: US30USD,   base: OANDA_US30USD }
          - { ticker: DE30EUR,   base: OANDA_DE30EUR }
          - { ticker: BTCUSD,    base: COINBASE_BTCUSD }
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Téléchargement ${{ matrix.ticker }}
        run: |
          # jusqu'à 4 passages : chaque passage reprend après la dernière bougie enregistrée
          for i in 1 2 3 4; do
            python historique_scanner.py --only ${{ matrix.ticker }} --start 2024-01-01 --out historique --threads 4 | tee log.txt
            if grep -q "Terminé" log.txt; then break; fi
            sleep 60
          done
          ls -la historique/ | grep ${{ matrix.base }} || true
      - uses: actions/upload-artifact@v4
        with:
          name: ${{ matrix.base }}
          path: historique/${{ matrix.base }}_*.csv.gz
          if-no-files-found: warn
          retention-days: 2

  enregistrement:
    needs: actif
    if: always()
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with:
          path: nouveaux
          merge-multiple: true
      - name: Enregistrer dans le dépôt
        run: |
          mkdir -p historique
          cp -f nouveaux/*.csv.gz historique/ 2>/dev/null || true
          git config user.name "goldneye-bot"
          git config user.email "goldneye-bot@users.noreply.github.com"
          git add historique/*.csv.gz
          if git diff --cached --quiet; then echo "Rien de nouveau."; exit 0; fi
          git commit -m "Historique scanner : mise à jour du $(date -u +%Y-%m-%d)"
          git push
