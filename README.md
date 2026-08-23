# Hydra Terminal — silnik sygnału + automatyczny, "żywy" dashboard

Reimplementacja mechanizmu obserwowanego na hydra.trading: klasyfikacja
portfeli DEX na "dobrych" i "złych" traderów na podstawie ich historycznej
skuteczności, i sygnał LONG/SHORT/HOLD dla ETH/USD na podstawie tego, co ta
pula GOOD/BAD kupuje albo sprzedaje w danym oknie czasu. Pełne uzasadnienie
metodologiczne i historia projektu (w tym oryginalny, wyciekniety
debug-dump z hydra.trading, na podstawie którego odtworzono format
wewnętrznych obliczeń) — w projekcie Claude, dokumenty
`hydrav2-engine-v1.md` i `hydrav2-frontend.md`.

To repo to **Etap 3**: strona, która **żyje sama** — GitHub Actions co
godzinę pobiera nowe transakcje z Uniswap V3 (Ethereum mainnet), dolicza je
do sygnału, i commituje zaktualizowaną stronę, którą Netlify automatycznie
wdraża. Zero serwera do utrzymania, zero kosztu, zero zależności od
czyjegokolwiek komputera będącego akurat włączonym.

## Struktura repo

```
hydra_signals/          silnik (testowany, 27 testów) - klasyfikacja portfeli + scoring
  wallets.py             PnL average-cost, ranking percentylowy, kohorty GOOD/BAD/NEUTRAL
  scoring.py             agregacja okienna (świece 250-blokowe) + EMA + decyzja sygnału
  models.py               typy danych (Trade, WalletStats, WindowScore, Signal)
  synthetic.py            generator syntetycznego rynku (walidacja bez realnych danych)
  backtest.py              prosty backtest na danych syntetycznych
  data_sources/
    pools.py               konfiguracja puli Uniswap V3 (zweryfikowana on-chain)
    onchain_rpc.py          klient JSON-RPC, dekoder eventu Swap, wsadowe zapytania z retry

live/                    automatyzacja "żywego" dashboardu
  state.py                 zapis/odczyt trwałego stanu (data/*)
  run_incremental.py        główny skrypt uruchamiany co godzinę przez Actions
  build_site.py             generuje site/index.html z historii świec
  template.html             szablon strony (design "Marvel-style", ten sam co artefakt na claude.ai)

data/                    trwały stan (generowany automatycznie, patrz data/README.md)
site/                    wygenerowana strona - TO Netlify wdraża (publish directory)
tests/                   27 testów jednostkowych + integracyjnych (pytest)
.github/workflows/
  update.yml               workflow GitHub Actions - cron co godzinę + uruchomienie ręczne
netlify.toml             mówi Netlify, że publikuje katalog `site/`, bez kroku budowania
run_live_pipeline.py     jednorazowy CLI do ad-hoc analizy (backtest na wybranym zakresie bloków)
```

## Jak uruchomić automatyzację (jednorazowa konfiguracja)

Potrzebujesz: konta GitHub (masz już to repo), darmowego klucza RPC z
Alchemy (masz już z poprzedniego etapu — `https://eth-mainnet.g.alchemy.com/v2/...`)
i darmowego konta Netlify.

**1. Dodaj kod do repo.** Rozpakuj przesłane archiwum i wrzuć jego
zawartość do tego repozytorium (przez `git push` z lokalnego klona, albo
przez "Add file → Upload files" na stronie repo na GitHub — jeśli używasz
przeglądania w przeglądarce, przeciągnij CAŁY rozpakowany folder na stronę
uploadu, żeby zachować strukturę podkatalogów).

**2. Dodaj sekret z kluczem RPC.** W repo na GitHub: *Settings → Secrets
and variables → Actions → New repository secret*. Nazwa:
`ALCHEMY_RPC_URL`. Wartość: Twój pełny URL RPC (z kluczem w środku, np.
`https://eth-mainnet.g.alchemy.com/v2/twoj_klucz`). To jedyne miejsce, gdzie
ten klucz się pojawia — nigdy nie trafia do kodu ani do historii commitów.

**3. Sprawdź uprawnienia workflow'u do zapisu.** *Settings → Actions →
General → Workflow permissions* → zaznacz **"Read and write permissions"**
→ Save. Bez tego workflow nie będzie mógł zacommitować zaktualizowanych
danych z powrotem do repo.

**4. Odpal pierwsze uruchomienie ręcznie.** Zakładka *Actions* → wybierz
workflow "Aktualizacja danych Hydra Terminal" → *Run workflow*. Pierwsze
uruchomienie robi jednorazowy backfill (~6500 bloków, kilka-kilkanaście
minut) i tworzy `data/` oraz `site/index.html` z prawdziwą zawartością.
Kolejne uruchomienia (co godzinę, automatycznie) są dużo szybsze — dociągają
tylko nowe bloki od ostatniego razu.

**5. Podłącz Netlify.** Na netlify.com: *Add new site → Import an existing
project → Deploy with GitHub* → wybierz to repo. Netlify samo wykryje
ustawienia z `netlify.toml` (publish directory: `site`, brak kroku
budowania). Każdy nowy commit na głównej gałęzi (czyli każde uruchomienie
workflow'u) automatycznie wdroży nową wersję strony pod tym samym adresem.

Od tego momentu strona żyje sama: co godzinę dociąga nowe transakcje,
przelicza sygnał, i publikuje się bez Twojego udziału.

## Jak to działa (skrót techniczny)

1. **Klasyfikacja portfeli** (`wallets.py`) — dla każdego portfela liczymy
   zrealizowany PnL metodą average-cost (obsługuje long i short) w oknie
   kroczącym ~6000 bloków, normalizujemy do PnL-na-ETH, dodajemy win-rate,
   i rankujemy percentylowo. Top ~15% → `GOOD`, dolne ~15% → `BAD`.

2. **Agregacja okienna i scoring** (`scoring.py`) — dla każdej świecy (250
   bloków, ~1h) liczymy, ilu portfeli z kohorty GOOD/BAD było netto
   kupujące vs sprzedające, wygładzamy EMA (krótki i długi horyzont), i
   łączymy w composite score: presja kupna w GOOD jest bycza, w BAD —
   kontrariańsko niedźwiedzia.

3. **Wznawialność** — `ScoringEngine` przyjmuje `initial_ema`,
   `initial_prev_signal`, `initial_total_tracked` i eksportuje stan przez
   `export_state()`. Dzięki temu osobny proces uruchamiany co godzinę (bez
   pamięci między uruchomieniami) może kontynuować dokładnie tam, gdzie
   skończył poprzedni — bez tego co godzinę sygnał "zapominałby" całą
   dotychczasową historię EMA i migotałby losowo. Numeracja okien jest
   liczona względem stałej, globalnej siatki bloków
   (`block // window_blocks`), nie względem pierwszego bloku w danym
   uruchomieniu — inaczej granice świec przesuwałyby się przy każdym
   restarcie procesu. Zweryfikowane testem `tests/test_resume.py`
   (podzielony przebieg == ciągły przebieg) i pełnym testem integracyjnym
   `tests/test_live_incremental.py` (dwa kolejne, symulowane uruchomienia
   procesu na fikcyjnym łańcuchu).

4. **Pobieranie danych on-chain** (`data_sources/onchain_rpc.py`) — czyta
   event `Swap` bezpośrednio z Uniswap V3 przez JSON-RPC. Alchemy (darmowy
   tier) narzuca twardy limit 10 bloków na jedno wywołanie `eth_getLogs`
   oraz throttling compute-units/s — `batch_call_with_retry` pakuje wiele
   wywołań w jedno żądanie HTTP (wsadowe JSON-RPC) i automatycznie ponawia
   pojedyncze wywołania, które dostały 429 albo padły sieciowo.

5. **`live/run_incremental.py`** — łączy powyższe w cykliczny krok:
   wczytaj stan → pobierz nowe transakcje → policz nowe świece → zapisz
   stan → wygeneruj `site/index.html`. Nie robi `git commit` sam — to
   celowo zostawione `update.yml`, żeby dało się przetestować lokalnie bez
   ryzyka przypadkowego commitu.

## Uruchomienie lokalne / rozwój

```bash
pip install -r requirements.txt
python -m pytest tests/ -v            # 27 testów - silnik, RPC, stan, integracja
python -m hydra_signals                # demo end-to-end na danych syntetycznych
python live/run_incremental.py        # jeden krok pipeline'u (wymaga ALCHEMY_RPC_URL w env)
```

Żeby przetestować `run_incremental.py` lokalnie bez czekania na Actions:

```bash
export ALCHEMY_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/twoj_klucz"
python live/run_incremental.py
open site/index.html   # albo: python -m http.server --directory site
```

## Ograniczenia i uczciwe zastrzeżenia

- **To badawcze narzędzie, nie porada inwestycyjna** (widoczne też w stopce
  strony).
- Automatyzacja startuje z historią ograniczoną do jednorazowego backfillu
  (~6500 bloków, ok. 27h) przy pierwszym uruchomieniu, i **rośnie od tego
  momentu w nieskończoność** — po kilku dniach przewyższy jednorazowy,
  ręczny przebieg (30 000 bloków) opisany w `hydrav2-engine-v1.md`, a po
  tygodniach/miesiącach będzie miała znacznie głębszą, w pełni ciągłą
  historię. Świadomie NIE próbowaliśmy "zaszczepić" automatyzacji danymi
  z tamtego ręcznego przebiegu — mieliśmy tylko zagregowane wyniki (świece),
  nie surowe transakcje ani prawdziwe adresy portfeli (były pseudonimizowane
  po drodze przez narzędzie do automatyzacji przeglądarki) — a bez tego
  ciągłość liczby "śledzonych portfeli" byłaby fikcyjna. Czysty start jest
  uczciwszy niż sklejenie dwóch niekompatybilnych źródeł.
- `data/wallets_seen.txt` rośnie z czasem (jeden adres = jedna linia) —
  przy typowym ruchu tej puli to lata, zanim rozmiar pliku w repo stanie
  się problemem, ale warto o tym pamiętać przy planowaniu bardzo
  długoterminowego działania (patrz `data/README.md`).
- `good_pct`/`bad_pct`, wagi EMA i `signal_threshold` w `ScoringConfig` to
  rozsądne wartości startowe, nie wynik optymalizacji na realnych danych.
- Jeśli workflow nie uruchomi się przez dłuższy czas (np. wyłączony na
  kilka dni), `HYDRA_MAX_NEW_BLOCKS_PER_RUN` (domyślnie 20 000) ogranicza,
  ile bloków jedno uruchomienie próbuje dogonić na raz — reszta zaległości
  dogoni się w kolejnych uruchomieniach, zamiast ryzykować przekroczenie
  limitu czasu joba.

## Co dalej

1. Strojenie hiperparametrów `ScoringConfig` na realnych, narastających
   danych (teraz możliwe — automatyzacja generuje coraz większą próbkę).
2. Rozważyć dodatkowe pule (więcej par handlowych) dla szerszego pokrycia
   portfeli.
3. Zwalidować sygnał względem historii z oryginalnego hydra.trading jako
   benchmark.
4. Strona biznesowa: model monetyzacji, zastrzeżenie "nie jest to porada
   inwestycyjna" do konsultacji prawnej przy komercjalizacji.
