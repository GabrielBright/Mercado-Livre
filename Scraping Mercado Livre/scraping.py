import os
import sys
import re
import asyncio
import logging
import pandas as pd
from tqdm import tqdm
from time import time
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Entrada
ARQUIVO_EXCEL_LINKS = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\links_ml.xlsx"

# Saída
ARQUIVO_PKL_DADOS   = "dados_ml.pkl"
ARQUIVO_EXCEL_DADOS = "dados_ml.xlsx"
ARQUIVO_CHECKPOINT  = "checkpoint_ml.pkl"

USE_LOGIN = True
STORAGE_STATE = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\ml_state.json"

CHECKPOINT_EVERY = 150

TIMEOUT_MS      = 90_000
RETRIES_PAGE    = 3
MAX_CONCURRENT  = 3

BAD_TEXTS = {
    "pular para o conteúdo", "ir para o conteúdo", "skip to content", "voltar ao topo",
}

def eh_texto_valido(txt: str) -> bool:
    if not txt: return False
    s = re.sub(r"\s+", " ", txt).strip().lower()
    return bool(s) and s not in BAD_TEXTS and len(s) > 2

def limpar_texto_num(txt: str) -> str:
    if txt is None: return ""
    return re.sub(r"\s+", " ", txt).strip()

def parse_preco(ptxt: str) -> Optional[float]:
    if not ptxt: return None
    ptxt = ptxt.replace("\xa0", " ").strip()
    m = re.search(r"(\d{1,3}(\.\d{3})*(,\d{2})?)", ptxt)
    if not m: return None
    n = m.group(1).replace(".", "").replace(",", ".")
    try: return float(n)
    except: return None

def parse_ano_km_from_header(header_text: str) -> (Optional[int], Optional[int]):
    if not header_text: return None, None
    s = header_text.replace("\xa0"," ").strip()
    ano = None
    ma = re.search(r"\b(19[5-9]\d|20[0-4]\d)\b", s)
    if ma:
        try: ano = int(ma.group(1))
        except: ano = None
    # km
    km = None
    mk = re.search(r"([\d\.\s]+)\s*km\b", s.lower())
    if mk:
        raw = re.sub(r"[^\d]", "", mk.group(1))
        if raw:
            try:
                val = int(raw)
                if 0 <= val <= 2_000_000:
                    km = val
            except:
                pass
    return ano, km

SELETORES_PRECO = [
    'meta[itemprop="price"]',
    "#price > div > div > div > span > span",
    "xpath=//*[@id='price']/div/div/div/span/span",
    "#price > div > div > div > span > span > span.andes-money-amount__fraction",
    "xpath=//*[@id='price']/div/div/div/span/span/span[2]",
    "css=span.andes-money-amount__fraction",
    "css=span.andes-money-amount__fraction + span.andes-money-amount__cents",
    "css=.ui-pdp-price__main-container .andes-money-amount__fraction",
    "css=.ui-pdp-price__second-line .andes-money-amount__fraction",
]

SEL_TITLE = [
    "#header h1", "h1.ui-pdp-title", "h1[itemprop='name']", "xpath=//*[@id='header']//h1"
]
SEL_HEADER_SUB = [
    "#header > div > div.ui-pdp-header__subtitle > span",
    "xpath=//*[@id='header']/div/div[1]/span"
]

SEL_MARCA = [
    r"#\:R2iesraacde\:", r"xpath=//*[@id=':R2iesraacde:']",
    r"#\:R2iesraacde\:-value", r"xpath=//*[@id=':R2iesraacde:-value']",
    '//*[@id=":R2iesraac5e:"]'
]
SEL_MODELO = [
    r"xpath=//*[@id=':R2imsraacde:-value']", r"#\:R2imsraacde\:-value",
    r"#\:R2imsraacde\:", r"xpath=//*[@id=':R2imsraacde:']",
    '//*[@id=":R2imsraac5e:"]', "#\:R2imsraac5e\:"
]
SEL_ANO = [
    r"#\:R2j6sraacde\:", r"xpath=//*[@id=':R2j6sraacde:']",
    r"#\:R2j6sraacde\:-value", r"xpath=//*[@id=':R2j6sraacde:-value']",
    r"#\:R2iusraacde\:", r"xpath=//*[@id=':R2iusraacde:']",
    r"#\:R2iusraacde\:-value", r"xpath=//*[@id=':R2iusraacde:-value']",
]
SEL_KM = [
    r"#\:R2jmsraacde\:-value", r"xpath=//*[@id=':R2jmsraacde:-value']",
    r"#\:R2jmsraacde\:", r"xpath=//*[@id=':R2jmsraacde:']",
    r"#\:R2jesraacde\:-value", r"xpath=//*[@id=':R2jesraacde:-value']",
    r"#\:R2jesraacde\:",       r"xpath=//*[@id=':R2jesraacde:']",
]
SEL_TIPO = [
    r"xpath=//*[@id='header']/div/div[2]/h1"
]

XPATH_POR_LABEL = {
    "Marca":  [
        "xpath=//*[contains(normalize-space(.), 'Marca')]/following::*[self::span or self::p or self::b][1]",
        "xpath=//th[contains(.,'Marca')]/following-sibling::td[1]",
        "xpath=//*[@role='table']//*[contains(.,'Marca')]/following::*[1]"
    ],
    "Modelo": [
        "xpath=//*[contains(normalize-space(.), 'Modelo')]/following::*[self::span or self::p or self::b][1]",
        "xpath=//th[contains(.,'Modelo')]/following-sibling::td[1]",
        "xpath=//*[@role='table']//*[contains(.,'Modelo')]/following::*[1]"
    ],
    "Ano":    [
        "xpath=//*[contains(normalize-space(.), 'Ano')]/following::*[self::span or self::p or self::b][1]",
        "xpath=//th[contains(.,'Ano')]/following-sibling::td[1]",
        "xpath=//*[@role='table']//*[contains(.,'Ano')]/following::*[1]"
    ],
    "Km":     [
        "xpath=//*[contains(normalize-space(.), 'Km') or contains(normalize-space(.), 'KM') or contains(normalize-space(.), 'Quilometragem')]/following::*[self::span or self::p or self::b][1]",
        "xpath=//th[contains(translate(., 'KMkm', 'KMKM'),'KM') or contains(translate(.,'quilometragem','QUILOMETRAGEM'),'QUILOMETRAGEM')]/following-sibling::td[1]",
        "xpath=//*[@role='table']//*[contains(translate(normalize-space(.), 'kmKM', 'KMKM'),'KM')]/following::*[1]"
    ],
    "Tipo": [
        "xpath=//*[@id='header']/div/div[2]/h1"
    ],
}

async def get_preco_estavel(page) -> Optional[float]:
    try:
        el = page.locator('meta[itemprop="price"]')
        if await el.count() > 0:
            val = await el.first.get_attribute("content")
            if val:
                try:
                    return float(val.replace(".", "").replace(",", "."))
                except:
                    pass
    except:
        pass
    preco_txt = await get_text(page, SELETORES_PRECO)
    return parse_preco(preco_txt)

async def fechar_modal_atributos_acessibilidade(page):
    for _ in range(3):
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(150)
    candidatos = [
        "button[aria-label='Fechar']",
        ".andes-modal__close",
        "button:has-text('Fechar')",
        "button:has-text('Ok')",
        "[role='dialog'] .andes-button:has-text('Fechar')",
    ]
    for sel in candidatos:
        try:
            btn = page.locator(sel).first
            if await btn.count() and await btn.is_visible():
                await btn.click()
                await page.wait_for_timeout(200)
        except:
            pass
    try:
        await page.evaluate("""
          for (const el of document.querySelectorAll(
            '[role=dialog], .andes-modal, .ui-dialog, .ui-dialog__container'
          )) { el.remove(); }
        """)
    except:
        pass

async def get_text(page, selectors: List[str], timeout_ms=TIMEOUT_MS) -> str:
    for sel in selectors:
        try:
            scope = page.locator("main") if await page.locator("main").count() else page
            is_x = sel.startswith("xpath=") or sel.startswith("//") or sel.startswith("xpath:/") or sel.startswith("/")
            locator = scope.locator(sel if is_x else sel)
            count = await locator.count()
            if count == 0: continue
            for i in range(min(count, 5)):
                el = locator.nth(i)
                try: await el.scroll_into_view_if_needed(timeout=timeout_ms)
                except: pass
                txt = None
                try: txt = await el.inner_text(timeout=timeout_ms)
                except:
                    try: txt = await el.text_content(timeout=timeout_ms)
                    except: txt = None
                if not txt: continue
                txt = limpar_texto_num(txt)
                if not eh_texto_valido(txt): continue
                return txt
        except Exception:
            continue
    return ""

async def scroll_stepwise(page, total_px=2000, step_px=800, pause_ms=350):
    scrolled = 0
    while scrolled < total_px:
        await page.evaluate(f"window.scrollBy(0, {step_px})")
        await page.wait_for_timeout(pause_ms)
        scrolled += step_px

async def esperar_anuncio(page):
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=TIMEOUT_MS)
        await page.wait_for_selector(
            "h1.ui-pdp-title, h1[itemprop='name'], #price, .ui-pdp-price, meta[itemprop='price']",
            timeout=TIMEOUT_MS
        )
        await scroll_stepwise(page, total_px=1600, step_px=800, pause_ms=350)
    except PWTimeout:
        await page.evaluate("window.scrollBy(0, 800)")
        await page.wait_for_timeout(1000)

async def extrair_anuncio(context, link: str) -> Optional[Dict[str, Any]]:
    page = await context.new_page()
    try:
        for tent in range(1, RETRIES_PAGE + 1):
            try:
                resp = await page.goto(link, timeout=TIMEOUT_MS, wait_until="domcontentloaded")
                if not resp or (resp.status and resp.status >= 400):
                    await asyncio.sleep(0.8 * tent)
                    continue

                await fechar_modal_atributos_acessibilidade(page)
                try:
                    await page.wait_for_load_state("networkidle", timeout=5_000)
                except:
                    pass

                await esperar_anuncio(page)

                # PREÇO
                preco_val = await get_preco_estavel(page)
                preco_txt = (
                    f"R$ {int(preco_val):,}".replace(",", ".")
                    if isinstance(preco_val, (int, float))
                    else await get_text(page, SELETORES_PRECO)
                )

                # Título / Header (Ano e KM)
                title_txt  = await get_text(page, SEL_TITLE)
                header_txt = await get_text(page, SEL_HEADER_SUB)
                ano_h, km_h = parse_ano_km_from_header(header_txt)

                # Marca/Modelo por seletores antigos (header pode não ter)
                marca  = await get_text(page, SEL_MARCA)  or await get_text(page, XPATH_POR_LABEL["Marca"])
                modelo = await get_text(page, SEL_MODELO) or await get_text(page, XPATH_POR_LABEL["Modelo"])

                # Ano/Km: usa header primeiro; se não vier, usa seletores antigos
                ano_txt = str(ano_h) if ano_h is not None else (await get_text(page, SEL_ANO) or await get_text(page, XPATH_POR_LABEL["Ano"]))
                km_txt  = (f"{km_h} km" if km_h is not None else (await get_text(page, SEL_KM)  or await get_text(page, XPATH_POR_LABEL["Km"])))
                
                tipo = await get_text(page, SEL_TIPO)
                
                if not tipo and "Tipo" in XPATH_POR_LABEL:
                    tipo = await get_text(page, XPATH_POR_LABEL["Tipo"])

                # Normaliza numéricos
                def parse_int_from(txt: str) -> Optional[int]:
                    if not txt: return None
                    m = re.search(r"\b(19[5-9]\d|20[0-4]\d)\b", txt)
                    if not m: return None
                    try: return int(m.group(1))
                    except: return None

                def parse_km(txt: str) -> Optional[int]:
                    if not txt: return None
                    s = txt.lower().replace("km", "")
                    s = re.sub(r"[^\d]", "", s)
                    if not s: return None
                    try:
                        val = int(s)
                        return val if 0 <= val <= 2_000_000 else None
                    except: return None

                ano_val = parse_int_from(ano_txt)
                km_val  = parse_km(km_txt)

                dados = {
                    "Link": link,
                    "Título": title_txt,
                    "Preço_txt": preco_txt,
                    "Preço": preco_val if preco_val is not None else "",
                    "Marca": marca,
                    "Modelo": modelo,
                    "Ano_txt": ano_txt or "",
                    "Ano": ano_val if ano_val is not None else "",
                    "Km_txt": km_txt or "",
                    "Km": km_val if km_val is not None else "",
                    "Tipo": tipo,
                }
                return dados

            except Exception as e:
                if tent >= RETRIES_PAGE:
                    logging.warning(f"[{link}] erro final: {e}")
                    return None
                await asyncio.sleep(0.8 * tent)
        return None
    finally:
        try: await page.close()
        except: pass

async def carregar_links(arquivo=ARQUIVO_EXCEL_LINKS) -> List[str]:
    if not os.path.exists(arquivo):
        logging.error(f"Arquivo {arquivo} não encontrado.")
        return []
    try:
        df = await asyncio.to_thread(pd.read_excel, arquivo)
        if "Link" not in df.columns:
            logging.error("Coluna 'Link' não encontrada.")
            return []
        links = df["Link"].dropna().astype(str).unique().tolist()
        logging.info(f"{len(links)} links únicos carregados.")
        return links
    except Exception as e:
        logging.error(f"Erro ao carregar links: {e}")
        return []

async def processar_links(links: List[str], max_concurrent=MAX_CONCURRENT) -> List[Dict[str, Any]]:
    start = time()
    dados_coletados: List[Dict[str, Any]] = []
    processados = set()

    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            dados_coletados = pd.read_pickle(ARQUIVO_CHECKPOINT).to_dict("records")
            processados = {d["Link"] for d in dados_coletados if "Link" in d}
            links = [l for l in links if l not in processados]
            logging.info(f"Checkpoint: {len(dados_coletados)} já salvos, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=40,
            args=[
                "--no-proxy-server",
                "--proxy-bypass-list=*",
                "--proxy-auto-detect=false",
                "--disable-features=IsolateOrigins,site-per-process",
                "--lang=pt-BR",
            ],
        )
        context_kwargs = dict(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 860},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
        )

        if USE_LOGIN:
            if not os.path.exists(STORAGE_STATE):
                raise FileNotFoundError(f"{STORAGE_STATE} não encontrado. Gere a sessão com --salvar-estado.")
            context_kwargs["storage_state"] = STORAGE_STATE

        context = await browser.new_context(**context_kwargs)
        context.set_default_timeout(TIMEOUT_MS)

        # Diagnóstico (opcional)
        diag = await context.new_page()
        await diag.goto("https://api.ipify.org?format=json", timeout=30000)
        print("IP externo:", await diag.text_content("pre, body"))
        await diag.close()

        sem = asyncio.Semaphore(max_concurrent)

        async def worker(url: str):
            async with sem:
                return await extrair_anuncio(context, url)

        tarefas = [asyncio.create_task(worker(u)) for u in links]
        for f in tqdm(asyncio.as_completed(tarefas), total=len(tarefas), desc="Coletando anúncios"):
            try:
                res = await f
                if res:
                    dados_coletados.append(res)
                    if len(dados_coletados) % CHECKPOINT_EVERY == 0:
                        pd.DataFrame(dados_coletados).to_pickle(ARQUIVO_CHECKPOINT)
                        logging.info(f"{len(dados_coletados)} salvos no checkpoint.")
            except Exception as e:
                logging.error(f"Erro em tarefa: {e}")

        await context.close()
        await browser.close()

    logging.info(f"Finalizado em {time() - start:.2f}s com {len(dados_coletados)} registros.")
    return dados_coletados

async def salvar_dados(dados: List[Dict[str, Any]]):
    if not dados:
        logging.warning("Nenhum dado para salvar.")
        return
    df = pd.DataFrame(dados)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"PKL salvo: {ARQUIVO_PKL_DADOS}")
    try:
        if os.path.exists(ARQUIVO_EXCEL_DADOS):
            df_existente = await asyncio.to_thread(pd.read_excel, ARQUIVO_EXCEL_DADOS, engine="openpyxl")
            df_final = pd.concat([df_existente, df], ignore_index=True)
        else:
            df_final = df
        await asyncio.to_thread(df_final.to_excel, ARQUIVO_EXCEL_DADOS, index=False, engine="openpyxl")
        logging.info(f"Excel salvo: {ARQUIVO_EXCEL_DADOS}")
    except Exception as e:
        logging.error(f"Erro ao salvar Excel: {e}")

async def salvar_estado_login():
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False, slow_mo=60,
            args=[
                "--no-proxy-server",
                "--proxy-bypass-list=*",
                "--proxy-auto-detect=false",
                "--disable-features=IsolateOrigins,site-per-process",
                "--lang=pt-BR",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 860},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
        )
        page = await context.new_page()
        await page.goto("https://www.mercadolivre.com.br/", timeout=120_000)
        print("Faça login manualmente nesta janela.")
        await asyncio.to_thread(input, "\nPressione Enter para salvar a sessão e fechar...\n")
        await context.storage_state(path=STORAGE_STATE)
        await browser.close()
        print(f"Estado salvo em: {STORAGE_STATE}")

async def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--salvar-estado":
        await salvar_estado_login()
        return

    links = await carregar_links()
    if not links:
        return
    dados = await processar_links(links)
    await salvar_dados(dados)

if __name__ == "__main__":
    asyncio.run(main())
