import asyncio
import random
import os
import sys
import re
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

URL_INICIAL = "https://lista.mercadolivre.com.br/veiculos/onibus/_YearRange_2002-0_NoIndex_True"
LIMITE_TOTAL = 3566
ARQUIVO_XLSX = "links_ml.xlsx"
ARQUIVO_CSV  = "links_ml.csv"

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

def construir_url_proxima(url_atual: str, coletados: int, por_pagina: int = ITENS_POR_PAGINA) -> str:
    proximo_desde = coletados + 1  # 1-based
    if "_Desde_" in url_atual:
        return re.sub(r"_Desde_\d+", f"_Desde_{proximo_desde}", url_atual)
    base, sep, resto = url_atual.partition("?")
    nova_base = f"{base.rstrip('/')}_Desde_{proximo_desde}"
    return f"{nova_base}{sep}{resto}" if sep else nova_base

async def abrir_pagina_busca(page, url):
    for tent in range(1, RETRIES_PAGINA + 1):
        try:
            await page.goto(url, timeout=PAGE_TIMEOUT_MS, referer="https://www.mercadolivre.com.br/")
            # se caiu em verificação, tente a listagem base de novo
            if "account-verification" in page.url or "/gz/" in page.url:
                await page.goto(URL_INICIAL, timeout=PAGE_TIMEOUT_MS, referer="https://www.mercadolivre.com.br/")
            await esperar_resultados(page)
            return True
        except PWTimeout:
            print(f"[WARN] Timeout ao carregar a busca ({tent}/{RETRIES_PAGINA})")
            await asyncio.sleep(1.5 * tent)
    return False

async def ir_para_proxima(page, total_coletados: int) -> bool:
    for tent in range(1, RETRIES_NEXT + 1):
        try:
            # rola até o rodapé para garantir que a paginação esteja visível
            await scroll_to_bottom(page)

            # primeiro card antes da navegação (para detectar mudança real)
            first_before = await page.locator(SEL_CARD).first.get_attribute("href")

            # tenta clicar no botão "próximo"
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

                    # espera trocar de cards
                    await page.wait_for_function(
                        "(arg) => { const a = document.querySelector(arg.sel); return a && a.href && a.href !== arg.before; }",
                        arg={"sel": SEL_CARD, "before": first_before},
                        timeout=PAGE_TIMEOUT_MS
                    )
                    await asyncio.sleep(random.uniform(*PAGE_NAV_EXTRA_WAIT_S))
                    await esperar_resultados(page)
                    return True
                except PWTimeout:
                    # se o clique não navegou, tenta ir pelo href do botão
                    try:
                        await page.goto(next_href, timeout=PAGE_TIMEOUT_MS, referer=page.url)
                        await asyncio.sleep(random.uniform(*PAGE_NAV_EXTRA_WAIT_S))
                        await esperar_resultados(page)
                        return True
                    except Exception as e:
                        print(f"[WARN] Falha ao navegar por href do próximo: {e}")

            # se não havia botão ou falhou, tenta construir URL com _Desde_
            url_fallback = construir_url_proxima(page.url, total_coletados, ITENS_POR_PAGINA)
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

        # backoff progressivo
        await asyncio.sleep(0.8 * tent)

    return False

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
        ok = await abrir_pagina_busca(page, URL_INICIAL)
        if not ok:
            print("[ERRO] Não consegui abrir a busca dentro do timeout.")
            await context.close()
            await browser.close()
            return

        pagina = 1
        while len(links) < LIMITE_TOTAL:
            print(f"\nPágina {pagina} — {len(links)}/{LIMITE_TOTAL} coletados")
            novos = await coletar_links_pagina(page, links, LIMITE_TOTAL)
            print(f"Novos nesta página: {novos}")

            if len(links) >= LIMITE_TOTAL:
                break

            tem_proxima = await ir_para_proxima(page, len(links))
            if not tem_proxima:
                print("Não há (ou não consegui abrir) a próxima página. Encerrando.")
                break

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