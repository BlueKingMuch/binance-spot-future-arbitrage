# Binance Spot-Futures Arbitrage Bot

**English version below**

## Deutsch

Ein Arbitrage-Bot für den Handel zwischen Binance Spot (Cross Margin) und USD-M Futures. Der Bot identifiziert, führt aus und verwaltet Arbitrage-Möglichkeiten, die durch Preisunterschiede zwischen diesen beiden Märkten entstehen. Er enthält außerdem ein vollautomatisches Wartungsmodul zur Kontenpflege.

### WICHTIGER HAFTUNGSAUSSCHLUSS

**DIES IST KEINE FINANZBERATUNG. DIE VERWENDUNG DIESES BOTS ERFOLGT AUF EIGENES RISIKO.** Der Handel mit Kryptowährungen ist hochriskant und kann zum vollständigen Verlust deines Kapitals führen. Der Autor übernimmt keine Haftung für finanzielle Verluste. **Teste diesen Bot ausgiebig in einer Simulationsumgebung (`LIVE_TRADING = False` in `config.yaml`), bevor du echtes Geld einsetzt.**

### Hauptfunktionen

*   **Echtzeit-Datenverarbeitung:** Nutzt WebSockets für Spot- und Futures-Märkte, um Preisdaten mit geringer Latenz zu erhalten.
*   **Dynamische Schwellenwerte:** Berechnet Einstiegsschwellen dynamisch unter Einbeziehung von Finanzierungskosten (Funding Rates) und Zinskosten (Margin Interest).
*   **Parallele Ausführung:** Führt Spot- und Futures-Orders gleichzeitig aus, um das Slippage-Risiko zu minimieren.
*   **Robustes Risikomanagement:** Beinhaltet Parameter für maximale Positionsgröße, Kapitalallokation und automatische Schließung von Positionen (z.B. bei zu langer Haltedauer oder Delisting-Gefahr).
*   **Automatisches Wartungsmodul:**
    *   Konvertiert "Staub" (kleine Restbeträge) im Margin-Konto zu BNB.
    *   Stellt sicher, dass ausreichend BNB für Gebühren vorhanden ist und kauft bei Bedarf nach.
    *   Gleicht Kapital (USDC) zwischen Margin- und Futures-Konten aus, um die Liquidität zu optimieren.
    *   Behandelt PNL-Sonderfälle im Futures-Konto (BNFCR).
*   **Detailliertes Logging:** Protokolliert alle wichtigen Aktionen, Trades und Kontostände zur einfachen Nachverfolgung.

### Installation & Einrichtung

1.  **Repository klonen:**
    ```bash
    git clone https://github.com/bluekingmuch/binance-spot-future-arbitrage.git
    cd binance-spot-future-arbitrage
    ```

2.  **Virtuelle Umgebung erstellen & aktivieren (empfohlen):**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # macOS / Linux
    source venv/bin/activate
    ```

3.  **Abhängigkeiten installieren:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **API-Keys konfigurieren:**
    *   Erstelle eine Kopie von `keys.yaml.example` und nenne sie `keys.yaml`.
    *   Trage deine Binance API-Keys in die `keys.yaml` ein. Stelle sicher, dass die Keys für den Margin- und Futures-Handel aktiviert sind.
    ```yaml
    API_KEY: "DEIN_API_KEY"
    API_SECRET: "DEIN_API_SECRET"
    ```

5.  **Strategie konfigurieren:**
    *   Öffne die `config.yaml` und passe die Parameter an deine Strategie und dein Risikoprofil an. Die Parameter sind innerhalb der Datei kommentiert.

### Verwendung

Führe den Bot über die Kommandozeile aus:

```bash
python bot.py
```

Für zusätzliche Debug-Ausgaben (zeigt die Top 5 potenziellen Opportunities periodisch an):
```bash
python bot.py --debug
```

---

## English

An arbitrage bot for trading between Binance Spot (Cross Margin) and USD-M Futures. The bot identifies, executes, and manages arbitrage opportunities arising from price discrepancies between these two markets. It also includes a fully automated maintenance module for account health.

### IMPORTANT DISCLAIMER 

**THIS IS NOT FINANCIAL ADVICE. USE THIS BOT AT YOUR OWN RISK.** Cryptocurrency trading is highly volatile and can result in the complete loss of your capital. The author assumes no liability for any financial losses. **Thoroughly test this bot in a simulated environment (`LIVE_TRADING = False` in `config.yaml`) before deploying real funds.**

### Core Features

*   **Real-time Data Processing:** Utilizes WebSockets for both Spot and Futures markets to get low-latency price data.
*   **Dynamic Thresholds:** Dynamically calculates entry thresholds by incorporating funding rates and margin interest costs.
*   **Parallel Execution:** Executes spot and futures orders simultaneously to minimize slippage risk.
*   **Robust Risk Management:** Includes parameters for max position size, capital allocation, and automatic position closing (e.g., based on holding time or delisting risk).
*   **Automated Maintenance Module:**
    *   Converts "dust" in the margin account to BNB.
    *   Ensures sufficient BNB for fees is available and buys more if needed.
    *   Balances capital (USDC) between margin and futures accounts to optimize liquidity.
    *   Handles PNL specifics in the futures account (BNFCR).
*   **Detailed Logging:** Logs all major actions, trades, and account states for easy monitoring.

### Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bluekingmuch/binance-spot-future-arbitrage.git
    cd binance-spot-future-arbitrage
    ```

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # macOS / Linux
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure API Keys:**
    *   Create a copy of `keys.yaml.example` and name it `keys.yaml`.
    *   Enter your Binance API keys into `keys.yaml`. Ensure the keys have permissions for margin and futures trading.
    ```yaml
    API_KEY: "YOUR_API_KEY"
    API_SECRET: "YOUR_API_SECRET"
    ```

5.  **Configure Strategy:**
    *   Open `config.yaml` and adjust the parameters to fit your strategy and risk profile. The parameters are commented within the file.

### Usage

Run the bot from your command line:

```bash
python bot.py
```

For additional debug output (periodically shows the top 5 potential opportunities):
```bash
python bot.py --debug
```

### License


This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

