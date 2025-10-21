import asyncio
import random
import os
import sys
import re
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

LIMITE_TOTAL = 3678
ARQUIVO_XLSX = "links_ml.xlsx"
ARQUIVO_CSV  = "links_ml.csv"

START_URLS = [
    "https://lista.mercadolivre.com.br/veiculos/outros/carretas-em-minas-gerais/#applied_filter_id%3Dstate%26applied_filter_name%3DLocaliza%C3%A7%C3%A3o%26applied_filter_order%3D3%26applied_value_id%3DTUxCUE1JTlMxNTAyZA%26applied_value_name%3DMinas+Gerais%26applied_value_order%3D5%26applied_value_results%3D357%26is_custom%3Dfalse",
    "https://lista.mercadolivre.com.br/veiculos/outros/carretas-em-parana/#applied_filter_id%3Dstate%26applied_filter_name%3DLocaliza%C3%A7%C3%A3o%26applied_filter_order%3D3%26applied_value_id%3DTUxCUFBBUkExODBlZA%26applied_value_name%3DParan%C3%A1%26applied_value_order%3D7%26applied_value_results%3D375%26is_custom%3Dfalse"
]

# Login persistente
USE_LOGIN = True
STORAGE_STATE = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\ml_state_links.json"

# Seletores
SEL_CARD = "a.poly-component__title"
SEL_NEXT = "li.andes-pagination__button.andes-pagination__button--next > a, a[rel='next']"

MIN_CARDS = 6
PAGE_TIMEOUT_MS = 90_000
RETRIES_PAGINA = 3
RETRIES_NEXT = 3

PAGE_NAV_EXTRA_WAIT_S = (1.0, 2.0)   
SCROLL_STEPS = 4                     
ITENS_POR_PAGINA = 48 

async def salvar_estado_login():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=80)
        context = await browser.new_context(
            viewport={"width": 1366, "height": 860},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
        )
        page = await context.new_page()
        await page.goto("https://www.mercadolivre.com.br/", timeout=120_000)
        print("Faça login manualmente nesta janela.")
        print("Quando estiver logado, volte ao terminal e pressione Enter para salvar a sessão.")
        await asyncio.to_thread(input, "\nPressione Enter para salvar a sessão e fechar...\n")
        await context.storage_state(path=STORAGE_STATE)
        await browser.close()
        print(f"Estado salvo em: {STORAGE_STATE}")

async def esperar_resultados(page, min_cards=MIN_CARDS, timeout_ms=PAGE_TIMEOUT_MS):
    await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    # pequeno scroll ajuda a acionar lazy-load
    await page.evaluate("window.scrollBy(0, 400)")
    await page.wait_for_function(
        "(arg) => document.querySelectorAll(arg.sel).length >= arg.n",
        arg={"sel": SEL_CARD, "n": min_cards},
        timeout=timeout_ms
    )
    await asyncio.sleep(random.uniform(0.4, 0.9))

async def coletar_links_pagina(page, links: set, limite: int) -> int:
    novos = 0
    cards = await page.locator(SEL_CARD).all()
    for a in cards:
        if len(links) >= limite:
            break
        href = await a.get_attribute("href")
        if href and "/MLB-" in href and href not in links:
            links.add(href)  # mantém com fragmento e query
            novos += 1
    # pequena espera para eventuais cards tardios
    await asyncio.sleep(0.2)
    return novos

async def scroll_to_bottom(page, steps=SCROLL_STEPS):
    # rola em etapas para disparar lazy-load e exibir paginação
    for _ in range(steps):
        await page.evaluate("window.scrollBy(0, document.body.scrollHeight)")
        await asyncio.sleep(0.35)

def construir_url_proxima(url_atual: str, cards_na_pagina: int, por_pagina: int = ITENS_POR_PAGINA) -> str:
    m = re.search(r"_Desde_(\d+)", url_atual)
    if m:
        atual = int(m.group(1))
        prox = atual + max(cards_na_pagina, 1)
        return re.sub(r"_Desde_\d+", f"_Desde_{prox}", url_atual)
    else:
        inicio = max(cards_na_pagina + 1, por_pagina + 1) if cards_na_pagina >= por_pagina else cards_na_pagina + 1
        base, sep, resto = url_atual.partition("?")
        nova_base = f"{base.rstrip('/')}_Desde_{inicio}"
        return f"{nova_base}{sep}{resto}" if sep else nova_base

async def abrir_pagina_busca(page, url):
    for tent in range(1, RETRIES_PAGINA + 1):
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT_MS, referer="https://www.mercadolivre.com.br/")
            if "account-verification" in page.url or "/gz/" in page.url:
                await page.goto(url, timeout=PAGE_TIMEOUT_MS, referer="https://www.mercadolivre.com.br/")
            await esperar_resultados(page)
            return True
        except PWTimeout:
            print(f"[WARN] Timeout ao carregar a busca ({tent}/{RETRIES_PAGINA}) | {url}")
            await asyncio.sleep(1.5 * tent)
    return False

async def ir_para_proxima(page, total_coletados: int, cards_na_pagina: int) -> bool:
    for tent in range(1, RETRIES_NEXT + 1):
        try:
            await scroll_to_bottom(page)

            first_before = await page.locator(SEL_CARD).first.get_attribute("href")

            next_a = page.locator(SEL_NEXT).first
            next_href = None
            try:
                if await next_a.count() > 0 and await next_a.is_visible():
                    next_href = await next_a.get_attribute("href")
            except:
                next_href = None

            if next_href:
                try:
                    await next_a.scroll_into_view_if_needed()
                    await next_a.hover()
                    await asyncio.sleep(0.35 * tent)
                    await next_a.click()

                    await page.wait_for_function(
                        "(arg) => { const a = document.querySelector(arg.sel); return a && a.href && a.href !== arg.before; }",
                        arg={"sel": SEL_CARD, "before": first_before},
                        timeout=PAGE_TIMEOUT_MS
                    )
                    await asyncio.sleep(random.uniform(*PAGE_NAV_EXTRA_WAIT_S))
                    await esperar_resultados(page)
                    return True
                except PWTimeout:
                    try:
                        await page.goto(next_href, timeout=PAGE_TIMEOUT_MS, referer=page.url)
                        await asyncio.sleep(random.uniform(*PAGE_NAV_EXTRA_WAIT_S))
                        await esperar_resultados(page)
                        return True
                    except Exception as e:
                        print(f"[WARN] Falha ao navegar por href do próximo: {e}")

            # FALLBACK: usa offset baseado nos cards realmente exibidos nesta página
            url_fallback = construir_url_proxima(page.url, cards_na_pagina, ITENS_POR_PAGINA)
            if url_fallback != page.url:
                await page.goto(url_fallback, timeout=PAGE_TIMEOUT_MS, referer=page.url)
                await page.wait_for_function(
                    "(arg) => { const a = document.querySelector(arg.sel); return a && a.href && a.href !== arg.before; }",
                    arg={"sel": SEL_CARD, "before": first_before},
                    timeout=PAGE_TIMEOUT_MS
                )
                await asyncio.sleep(random.uniform(*PAGE_NAV_EXTRA_WAIT_S))
                await esperar_resultados(page)
                return True

        except PWTimeout:
            print(f"[WARN] Próxima página demorou (tentativa {tent}/{RETRIES_NEXT})")
        except Exception as e:
            print(f"[WARN] Falha ao ir para próxima (tentativa {tent}/{RETRIES_NEXT}): {e}")

        await asyncio.sleep(0.8 * tent)

    return False

async def reopen_if_closed(page, context, fallback_url: str):
    if page.is_closed():
        new_page = await context.new_page()
        ok = await abrir_pagina_busca(new_page, fallback_url)
        if not ok:
            raise RuntimeError(f"Falha ao reabrir listagem: {fallback_url}")
        return new_page
    return page

async def main():
    # utilitário para salvar sessão
    if len(sys.argv) > 1 and sys.argv[1] == "--salvar-estado":
        await salvar_estado_login()
        return

    links = set()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  
        context_kwargs = dict(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
            viewport={"width": 1366, "height": 860},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
        )
        if USE_LOGIN:
            if not os.path.exists(STORAGE_STATE):
                print(f"[INFO] {STORAGE_STATE} não encontrado. Abrindo janela para salvar sessão…")
                await salvar_estado_login()
            context_kwargs["storage_state"] = STORAGE_STATE

        context = await browser.new_context(**context_kwargs)
        context.set_default_timeout(PAGE_TIMEOUT_MS)

        page = await context.new_page()

        for seed_idx, seed_url in enumerate(START_URLS, start=1):
            if len(links) >= LIMITE_TOTAL:
                break

            print(f"\n=== Seed {seed_idx}/{len(START_URLS)} ===")
            ok = await abrir_pagina_busca(page, seed_url)
            if not ok:
                print(f"[WARN] Não consegui abrir a seed: {seed_url}. Pulando.")
                continue

            # guarda a última URL “saudável” da listagem
            last_listing_url = page.url

            pagina = 1
            while len(links) < LIMITE_TOTAL:
                print(f"Seed {seed_idx} | Página {pagina} — {len(links)}/{LIMITE_TOTAL} coletados")

                # --- contagem de cards com tolerância a fechamento ---
                try:
                    cards_count = await page.locator(SEL_CARD).count()
                except Exception:
                    # reabre e tenta de novo
                    page = await reopen_if_closed(page, context, last_listing_url)
                    cards_count = await page.locator(SEL_CARD).count()

                # --- coleta links com tolerância a fechamento ---
                try:
                    novos = await coletar_links_pagina(page, links, LIMITE_TOTAL)
                except Exception:
                    page = await reopen_if_closed(page, context, last_listing_url)
                    novos = await coletar_links_pagina(page, links, LIMITE_TOTAL)

                print(f"Novos nesta página: {novos}")

                if len(links) >= LIMITE_TOTAL:
                    break

                # fim desta seed?
                if cards_count < ITENS_POR_PAGINA and novos == 0:
                    print("Parece ser a última página desta seed.")
                    break

                # tenta ir para próxima; se a página fechar, reabre e continua
                try:
                    tem_proxima = await ir_para_proxima(page, len(links), cards_count)
                except Exception:
                    page = await reopen_if_closed(page, context, last_listing_url)
                    tem_proxima = await ir_para_proxima(page, len(links), cards_count)

                if not tem_proxima:
                    print("Não há (ou não consegui abrir) a próxima página nesta seed.")
                    break

                # navegou com sucesso — atualiza a última URL “saudável”
                last_listing_url = page.url

                pagina += 1
                await asyncio.sleep(random.uniform(0.8, 1.6))

            await context.close()
            await browser.close()

    # salvar resultados
    df = pd.DataFrame(sorted(links), columns=["Link"])
    df.to_excel(ARQUIVO_XLSX, index=False)
    df.to_csv(ARQUIVO_CSV, index=False, encoding="utf-8", sep=";")
    print(f"\nTotal coletado: {len(links)}")
    print(f"Arquivos: {ARQUIVO_XLSX} | {ARQUIVO_CSV}")

if __name__ == "__main__":
    asyncio.run(main())