# `data/` — trwały stan pipeline'u (generowany automatycznie)

Ten katalog jest celowo pusty na starcie. Po pierwszym udanym uruchomieniu
workflow'u `.github/workflows/update.yml` pojawią się tu cztery pliki,
tworzone i aktualizowane automatycznie przez `live/run_incremental.py`:

- `scoring_state.json` — stan silnika scoringu (EMA, ostatni sygnał, numer
  ostatnio przetworzonego bloku). Mały, nadpisywany co uruchomienie.
- `trade_buffer.csv` — transakcje z ostatnich ~6000 bloków (okno
  klasyfikacji portfeli). Rolujący bufor — stary rozmiar mniej więcej
  się utrzymuje, nie rośnie w nieskończoność.
- `wallets_seen.txt` — lista wszystkich unikalnych adresów portfeli
  kiedykolwiek zaobserwowanych (od pierwszego uruchomienia automatyzacji).
  Rośnie z czasem, ale wolno.
- `candles_history.json` — pełna, narastająca historia świec (scoringu)
  do wyświetlenia na stronie. `live/build_site.py` pokazuje na dashboardzie
  tylko ostatnie `MAX_DISPLAY_CANDLES` (domyślnie 500), więc strona zostaje
  lekka nawet po miesiącach działania.

Nie edytuj tych plików ręcznie — każde uruchomienie workflow'u je nadpisuje
na podstawie tego, co realnie odczyta z łańcucha. Więcej kontekstu:
`README.md` w katalogu głównym repo oraz komentarze w `live/state.py`.
