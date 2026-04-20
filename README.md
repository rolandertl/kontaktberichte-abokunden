# Kontaktberichte bei Abo-Kunden

Diese Streamlit-App wertet aktive Abo-Kunden aus und zeigt, wann zuletzt Kontakt mit ihnen stattgefunden hat.

## Funktionen

- Upload für aktuelle Auftragsliste
- Upload für aktuelle Kontaktberichte
- Auswertung nur für Verkäufer aus `aktive_verkaeufer.txt`
- Matching über `Kundennummer`, danach `Herold-Nummer`, danach Firmenname
- Anzeige von letztem Kontakt, letztem erfolgreichen Kontakt, Kontaktversuchen und Kontaktfrequenz
- Einstellbare Schonfrist für Kunden ohne bisherigen Kontakt nach Erstauftrag

## Lokal starten

```bash
pip install -r requirements.txt
streamlit run app.py
```

Unter macOS kann die App auch per Doppelklick über `Kontaktberichte starten.command` gestartet werden.

## Deployment auf Streamlit Community Cloud

1. Repository auf GitHub hochladen
2. In [streamlit.app](https://streamlit.app/) das Repository auswählen
3. Als Main File `app.py` angeben
4. Sicherstellen, dass `aktive_verkaeufer.txt` im Repository enthalten ist

Die lokalen Test-Exporte werden bewusst nicht versioniert.
