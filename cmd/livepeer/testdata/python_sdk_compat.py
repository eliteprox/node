"""Exercise the pinned, unmodified Live Runner Python SDK against a real server."""

import asyncio
import sys

import aiohttp

from livepeer_gateway.live_runner import register_runner
from livepeer_gateway.trickle_publisher import TricklePublisher
from livepeer_gateway.trickle_subscriber import TrickleSubscriber


async def check(orchestrator: str, runner_url: str, bootstrap: str, mode: str, proxy_mode: bool) -> None:
    reserved: asyncio.Queue[str] = asyncio.Queue()
    released: asyncio.Queue[str] = asyncio.Queue()

    async def on_reserve(event: object) -> None:
        await reserved.put(event.session_id)

    async def on_release(event: object) -> None:
        await released.put(event.session_id)

    registration = await register_runner(
        orchestrator,
        secret=bootstrap,
        runner_url=runner_url,
        app="python-sdk-compat",
        mode=mode,
        proxy=proxy_mode,
        auto_detect_gpu=False,
        heartbeat_interval_s=0.2,
        on_session_reserve=on_reserve,
        on_session_release=on_release,
    )
    try:
        assert registration.runner_id
        assert registration.o2r_channel
        async with aiohttp.ClientSession() as client:
            async with asyncio.timeout(5):
                while True:
                    async with client.get(f"{orchestrator}/discovery") as response:
                        entries = await response.json()
                    entry = next((item for group in entries for item in group.get("runners", [])
                                  if item["app"] == "python-sdk-compat"), None)
                    if entry is not None:
                        break
                    await asyncio.sleep(0.02)
            if mode == "single-shot":
                async with client.get(entry["url"] + "/hello") as response:
                    assert response.status == 200, await response.text()
                    assert await response.text() == "from-runner"
                session_id = await asyncio.wait_for(reserved.get(), 5)
                assert await asyncio.wait_for(released.get(), 5) == session_id
                return
            async with client.post(
                f"{orchestrator}/apps/{registration.runner_id}/session"
            ) as response:
                assert response.status == 200, await response.text()
                session = await response.json()
            session_id = session["session_id"]
            assert await asyncio.wait_for(reserved.get(), 5) == session_id
            assert session_id in registration.active_session_ids
            async with client.get(session["app_url"] + "/hello") as response:
                assert response.status == 200, await response.text()
                assert await response.text() == "from-runner"
            async with client.get(session["app_url"] + "/credentials") as response:
                assert response.status == 200, await response.text()
                token = (await response.json())["token"]
            proxy = await registration.create_proxy(
                session_id, runner_url, session_token=token
            )
            async with client.get(proxy.url + "/hello") as response:
                assert response.status == 200, await response.text()
                assert await response.text() == "from-runner"
            channels = await registration.create_trickle_channels(
                session_id,
                [{"name": "events", "mime_type": "application/json"}],
                session_token=token,
            )
            assert len(channels) == 1
            channel = channels[0]
            async with client.post(channel["url"] + "/0", data=b'{"ok":true}') as response:
                assert response.status == 200, await response.text()
            async with client.get(channel["url"] + "/0") as response:
                assert response.status == 200, await response.text()
                assert await response.json() == {"ok": True}
            deleted = await registration.remove_trickle_channels(
                session_id, [channel["channel_name"]], session_token=token
            )
            assert deleted == [channel["channel_name"]]
            published = await registration.create_trickle_channels(
                session_id,
                [{"name": "publisher", "mime_type": "application/json"}],
                session_token=token,
            )
            async with TricklePublisher(
                published[0]["url"], "application/json"
            ) as publisher:
                await publisher.create()
                async with await publisher.next() as segment:
                    await segment.write(b'{"published":true}')
                async with client.get(published[0]["url"] + "/0") as response:
                    assert response.status == 200, await response.text()
                    assert await response.json() == {"published": True}
                async with TrickleSubscriber(
                    published[0]["url"], start_seq=0
                ) as subscriber:
                    received = await asyncio.wait_for(subscriber.next(), 5)
                    assert received is not None
                    cursor = received.make_reader()
                    assert await cursor.read() == b'{"published":true}'
                    await received.close()
            async with client.post(session["control_url"] + "/stop") as response:
                assert response.status == 200, await response.text()
            assert await asyncio.wait_for(released.get(), 5) == session_id
            assert session_id not in registration.active_session_ids
    finally:
        await registration.close()
    print("Python SDK registration, heartbeat, reserve/release callbacks and unregister passed")


async def main(orchestrator: str, runner_url: str, bootstrap: str) -> None:
    for mode in ("persistent", "single-shot"):
        for proxy in (False, True):
            await check(orchestrator, runner_url, bootstrap, mode, proxy)
    print("Python SDK registration mode/proxy matrix passed")


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
