# acestream-proxy

Прокси для Ace Stream на Rust: несколько устройств могут смотреть одни и те же или разные трансляции (одна сессия движка на трансляцию), REST API со Swagger для будущего веб-интерфейса, воспроизведение в браузере с транскодингом в реальном времени и выбором аудиодорожек.

Аналог [HTTPAceProxy](https://github.com/pepsik-kiev/HTTPAceProxy), но с шарингом потоков, API и поддержкой браузера.

## Архитектура

```
устройства (VLC/TV) ──TS──┐
                          ├──▶ proxy (этот сервис) ──HTTP──▶ Ace Stream Engine (:6878)
браузер (hls.js) ──HLS────┘                                    (отдельный контейнер)
```

Движок не переизобретается: прокси управляет его сессиями (`/ace/getstream`), раздаёт байты клиентам и транскодит ffmpeg'ом только то, что браузер не играет.

## Возможности

- **Шаринг сессий**: N устройств на одной трансляции = 1 сессия движка, байты раздаются через broadcast
- **TS pass-through** для плееров: `GET /stream/{id}`
- **HLS pass-through** от движка: `GET /stream/{id}/index.m3u8`
- **Браузерное воспроизведение**: `GET /play/{id}/master.m3u8`
  - кодеки браузерные (h264+aac/mp3) → редирект на HLS движка (0 CPU)
  - иначе → транскод ffmpeg (h264+aac), все аудиодорожки — как alternate AUDIO renditions в master-плейлисте (hls.js переключает через `hls.audioTrack`)
- **Плейлист со всеми каналами**: `GET /playlist.m3u` — url ведут на прокси, категории через `group-title`; `?mode=transcode` — ссылки сразу на транскод (H.264+AAC HLS для слабых устройств)
- **API + Swagger**: `http://localhost:8080/swagger-ui`
- Автоочистка: стрим без зрителей гаснет через `STREAM_IDLE_SECS`; HLS/браузерные клиенты без запросов дропаются через `CLIENT_STALE_SECS`

## Запуск

```bash
docker compose up --build
# с плейлистом:
PLAYLIST_URL="https://example.com/playlist.m3u" docker compose up --build
```

`ENGINE_URL` должен указывать на HTTP API уже работающего движка (например `http://host.docker.internal:6878` или IP хоста/контейнера с движком).

Локально (нужен ffmpeg в PATH):

```bash
cargo build --release
ENGINE_URL=http://127.0.0.1:6878 ./target/release/acestream-proxy
```

## Конфигурация (env)

| Переменная | По умолчанию | Описание |
|---|---|---|
| `ACESTREAM_URI` | `127.0.0.1` | движок: ip или dns, схема/порт не обязательны. Когда задан явно, плейлист по умолчанию берётся с `http://<хост>:80/playlist.m3u` |
| `ACESTREAM_PLAYLIST_URL` | — | источник плейлиста: `http(s)://...` или `file:///...`. Качается в свой `TMP_DIR`, хранится там, переживает рестарт |
| `ACESTREAM_PLAYLIST_TTL_SECS` | `21600` (6ч) | срок жизни плейлиста до обновления; при ошибке загрузки retry каждые 60с |
| `PLAYLIST_REFRESH_SECS` | `1800` | период обновления плейлиста |
| `STREAM_IDLE_SECS` | `30` | сколько стрим живёт без зрителей |
| `CLIENT_STALE_SECS` | `60` | HLS/браузер-клиент считается ушедшим |
| `TRANSCODE_LIMIT` | кол-во ядер | максимум одновременных транскодов |
| `TMP_DIR` | `/tmp/acestream-proxy` | сегменты ffmpeg |
| `PUBLIC_URL` | Host-заголовок | override хоста в `/playlist.m3u` |

## API (кратко)

| Метод | Путь | Описание |
|---|---|---|
| GET | `/api/health` | статус, движок, версия |
| POST | `/api/streams` | старт: `{"content": "<32hex \\| url \\| magnet \\| channel-id>"}` |
| GET | `/api/streams` | активные трансляции + кто смотрит |
| GET/DELETE | `/api/streams/{id}` | инфо / убить стрим |
| GET | `/api/streams/{id}/audio` | аудиодорожки + нужен ли транскод |
| GET | `/api/clients` | все клиенты и что смотрят |
| GET | `/api/channels?group=&country=` | каналы (фильтр по подстроке) |
| GET | `/api/channels/groups` | группы каналов с количеством |
| GET | `/playlist.m3u` | плейлист для VLC/TV |

Полная схема: `/swagger-ui`, OpenAPI json: `/api/openapi.json`. CORS открыт — веб-интерфейс можно размещать отдельно.

## Заметки

- Сессии движка принудительно не останавливаются (API движка не документирован) — движок убирает их сам по своему таймауту после разрыва соединений.
- `magnet:` передаётся движку как `url=`; если ваша версия движка его не принимает — используйте content-id/torrent-url.
