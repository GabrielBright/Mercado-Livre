import os
import sys
import re
import asyncio
import logging
import pandas as pd
import unicodedata
from typing import Optional, Tuple
from tqdm import tqdm
from time import time
from typing import List, Dict, Any, Optional
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

sys.stdout.reconfigure(encoding='utf-8')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Entrada
ARQUIVO_EXCEL_LINKS = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\Faltando.xlsx"

# Saida
ARQUIVO_PKL_DADOS   = "dadosss_ml.pkl"
ARQUIVO_EXCEL_DADOS = "dadosasdas_ml.xlsx"
ARQUIVO_CHECKPOINT  = "checkpoint_ml.pkl"

USE_LOGIN = True  # use a sessão salva
STORAGE_STATE = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\ml_state.json"

CHECKPOINT_EVERY = 125

TIMEOUT_MS      = 100_000
RETRIES_PAGE    = 2
MAX_CONCURRENT  = 2

async def get_preco_estavel(page) -> Optional[float]:
    # tenta meta itemprop=price (varia entre ML países, mas ajuda)
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
    # fallback: seus seletores
    preco_txt = await get_text(page, SELETORES_PRECO)
    return parse_preco(preco_txt)

SELETORES_PRECO = [
    "#price > div > div > div > span > span",
    "xpath=//*[@id='price']/div/div/div/span/span",
    "#price > div > div > div > span > span > span.andes-money-amount__fraction",
    "xpath=//*[@id='price']/div/div/div/span/span/span[2]",
    "css=span.andes-money-amount__fraction",
    "css=span.andes-money-amount__fraction + span.andes-money-amount__cents",
    "css=.ui-pdp-price__main-container .andes-money-amount__fraction",
    "css=.ui-pdp-price__second-line .andes-money-amount__fraction",
]

SEL_MARCA = [
    r"#\:R2iesraacde\:", r"xpath=//*[@id=':R2iesraacde:']",
    r"#\:R2iesraacde\:-value", r"xpath=//*[@id=':R2iesraacde:-value']",
    '//*[@id=":R2iesraac5e:"]'
]
SEL_MODELO = [
    r"xpath=//*[@id=':R2imsraacde:-value']", r"#\:R2imsraacde\:-value",
    r"#\:R2imsraacde\:", r"xpath=//*[@id=':R2imsraacde:']",
    '//*[@id=":R2imsraac5e:"]'
    "#\:R2imsraac5e\:"
    
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
    r"#\:R2jesraacde\:-value", r"xpath=//*[@id=':R2jesraacde:-value']",
    r"#\:R2jesraacde\:",       r"xpath=//*[@id=':R2jesraacde:']",
]

SEL_TITLE = [
    "h1.ui-pdp-title",
    "h1[itemprop='name']",
    "#header h1",
    "xpath=//*[@id='header']//h1",
]   

SEL_HEADER_SUB = [
    "#header > div > div.ui-pdp-header__subtitle > span",
    "xpath=//*[@id='header']//div[contains(@class,'ui-pdp-header__subtitle')]//span",
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
}

BAD_TEXTS = {
    "pular para o conteúdo",
    "ir para o conteúdo",
    "skip to content",
    "voltar ao topo",
}

def eh_texto_valido(txt: str) -> bool:
    if not txt:
        return False
    s = re.sub(r"\s+", " ", txt).strip().lower()
    if not s:
        return False
    if s in BAD_TEXTS:
        return False
    if len(s) <= 2:
        return False
    return True

def limpar_texto_num(txt: str) -> str:
    if txt is None:
        return ""
    return re.sub(r"\s+", " ", txt).strip()

def parse_preco(ptxt: str) -> Optional[float]:
    if not ptxt:
        return None
    ptxt = ptxt.replace("\xa0", " ").strip()

    m = re.search(r"(\d{1,3}(\.\d{3})*(,\d{2})?)", ptxt)
    if not m:
        return None
    n = m.group(1)
    n = n.replace(".", "").replace(",", ".")
    try:
        return float(n)
    except:
        return None
    
def parse_int_from(txt: str) -> Optional[int]:
    if not txt:
        return None
    m = re.search(r"\b(19[5-9]\d|20[0-4]\d)\b", txt)  # anos 1950–2049
    if not m:
        return None
    try:
        return int(m.group(1))
    except:
        return None

def parse_km(txt: str) -> Optional[int]:
    if not txt:
        return None
    s = txt.lower()
    # remove “km”, pontos, vírgulas, espaços
    s = re.sub(r"km", "", s)
    s = re.sub(r"[^\d]", "", s)
    if not s:
        return None
    try:
        val = int(s)
        # sanity check: 0–2 milhões
        if 0 <= val <= 2_000_000:
            return val
    except:
        pass
    return None

def parse_ano_km_from_header(txt: str) -> tuple[Optional[int], Optional[int]]:
    if not txt:
        return None, None
    s = re.sub(r"\s+", " ", txt).strip().lower()
    # ano: 1950–2049
    m_year = re.search(r"\b(19[5-9]\d|20[0-4]\d)\b", s)
    ano = int(m_year.group(1)) if m_year else None
    # km: número seguido de 'km'
    m_km = re.search(r"(\d{1,3}(?:\.\d{3})*|\d+)\s*km\b", s)
    km = None
    if m_km:
        n = m_km.group(1).replace(".", "").replace(",", "")
        try:
            km = int(n)
        except:
            km = None
    return ano, km

async def fechar_modal_atributos_acessibilidade(page):
    # 1) tenta ESC algumas vezes
    for _ in range(3):
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(150)

    # 2) tenta clicar em botões de fechar comuns
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

    # 3) se ainda estiver aberto, remova forçadamente os diálogos
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
            # escopo: só dentro do <main> pra evitar header/skip-links
            scope = page.locator("main") if await page.locator("main").count() else page

            locator = (
                scope.locator(sel if sel.startswith("xpath=") or sel.startswith("//") else sel)
                if (sel.startswith("xpath=") or sel.startswith("//") or sel.startswith("xpath:/") or sel.startswith("/"))
                else scope.locator(sel)
            )

            count = await locator.count()
            if count == 0:
                continue

            for i in range(min(count, 5)):
                el = locator.nth(i)
                try:
                    await el.scroll_into_view_if_needed(timeout=timeout_ms)
                except:
                    pass

                txt = None
                try:
                    txt = await el.inner_text(timeout=timeout_ms)
                except:
                    try:
                        txt = await el.text_content(timeout=timeout_ms)
                    except:
                        txt = None

                if not txt:
                    continue

                txt = limpar_texto_num(txt)
                if not eh_texto_valido(txt):
                    continue

                return txt
        except Exception:
            continue
    return ""

async def scroll_stepwise(page, total_px=5000, step_px=800, pause_ms=350):
    scrolled = 0
    while scrolled < total_px:
        await page.evaluate(f"window.scrollBy(0, {step_px})")
        await page.wait_for_timeout(pause_ms)
        scrolled += step_px

async def scroll_until(page, selector_or_fn_js, timeout_ms=TIMEOUT_MS, step_px=800, pause_ms=350):
    deadline = page.timeouts()["default"] if hasattr(page, "timeouts") else None  # best-effort
    elapsed = 0
    chunk = 700  # ms
    while elapsed < timeout_ms:
        # tenta primeiro por seletor
        if isinstance(selector_or_fn_js, str) and not selector_or_fn_js.strip().startswith("("):
            try:
                el = page.locator(selector_or_fn_js)
                if await el.count() > 0 and await el.first.is_visible():
                    return True
            except:
                pass
        else:
            # função JS que deve retornar boolean
            try:
                ok = await page.evaluate(selector_or_fn_js)
                if ok:
                    return True
            except:
                pass

        await page.evaluate(f"window.scrollBy(0, {step_px})")
        await page.wait_for_timeout(pause_ms)
        elapsed += pause_ms
    return False

async def esperar_anuncio(page, timeout_ms=TIMEOUT_MS):
    # fecha/ignora overlays e “Pular para o conteúdo”
    await fechar_modal_atributos_acessibilidade(page)

    # estado mínimo carregado
    await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)

    # espera título ou preço estar presente na árvore
    await page.wait_for_selector(
        "h1.ui-pdp-title, h1[itemprop='name'], meta[itemprop='price'], .ui-pdp-price",
        timeout=timeout_ms
    )

    # dá uma respirada na rede (opcional, ajuda em SPA)
    try:
        await page.wait_for_load_state("networkidle", timeout=5_000)
    except:
        pass

    # rola um pouco pra acionar lazy-load
    for _ in range(3):
        await page.evaluate("window.scrollBy(0, 800)")
        await page.wait_for_timeout(350)

    # se houver, rola até “Características…”
    try:
        caract = page.locator("h2:has-text('Características'), h3:has-text('Características')")
        if await caract.count() > 0:
            await caract.first.scroll_into_view_if_needed()
            await page.wait_for_timeout(400)
    except:
        pass

    # aguarda a tabela de especificações ficar pronta (com linhas)
    await page.wait_for_function(
        """() => {
            const tbl = document.querySelector('table.andes-table');
            if (!tbl) return false;
            const body = tbl.querySelector('tbody');
            if (!body) return false;
            return body.querySelectorAll('tr').length >= 3; // Marca/Modelo/Ano etc.
        }""",
        timeout=timeout_ms
    )

    # margem pro layout estabilizar
    await page.wait_for_timeout(250)
        
async def salvar_estado_login():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=80)
        context = await browser.new_context(
            viewport={"width": 1366, "height": 860},
            locale="pt-BR",
            extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
            timezone_id="America/Sao_Paulo",
        )
        page = await context.new_page()
        await page.goto("https://www.mercadolivre.com.br/", timeout=120_000)
        print("Faça login manualmente nesta janela.")
        print("Quando a sua conta estiver logada (você consegue ver o nome/conta no topo),")
        print("volte ao terminal e pressione Enter para salvar a sessão.")
        # pausa até você apertar Enter no terminal
        await asyncio.to_thread(input, "\nPressione Enter para salvar o estado e fechar...\n")
        await context.storage_state(path=STORAGE_STATE)
        await browser.close()
        print(f"Estado salvo em: {STORAGE_STATE}")
        
BRANDS_OFFICIAL = [
    "MARCOPOLO","MERCEDES-BENZ","VOLKSWAGEN","SCANIA","IVECO","VOLVO","RENAULT","FORD",
    "CITROEN","JAC","AGRALE","INDUSCAR","BYD","LVTONG","HIGER","PEUGEOT","HYUNDAI",
    "ANKAI","CHEVROLET","SR","MOTOR-CASA","MA" 
]

# Aliases comuns -> marca oficial
BRAND_ALIASES = {
    "MERCEDES BENZ": "MERCEDES-BENZ",
    "MERCEDES": "MERCEDES-BENZ",
    "MB": "MERCEDES-BENZ",
    "VW": "VOLKSWAGEN",
    "VOLKSBUS": "VOLKSWAGEN",
}

# Modelos conhecidos (normalizados, quanto mais melhor)
KNOWN_MODELS = [
    "VOLARE V8L","VOLARE ACCESS","VOLARE DW9","VOLARE W-L","VOLARE V9L","VOLARE W9C",
    "VOLARE DV9L","VOLARE V6","VOLARE V10L","VOLARE MV8L","VOLARE W12","VOL 8L",
    "VOLARE A5","VOLARE TCA","VOLARE CINCO","ATTIVI","SENIOR","APACHE","MILLENNIUM",
    "COMIL","NEOBUS","BUSSCAR","ITALBUS","INDUSCAR","VETTURA","IRIZAR","E-VOLKSBUS",
    "CITY CLASS","WAYCLASS","GCLASS","RONTAN","TAKO","TCA","S312","OF 1519","K380","K310",
    "SPRINTER","MASTER","TRANSIT","DUCATO","BOXER","SCUDO","JUMPY","EXPERT","STARIA",
    "DAILY 45","DAILY 50","DAILY 55","DAILY", "B7",
    "IEV750","AZURE","EON","FE10","OE10","OE12","OE9","OE8","OE6","LT-S14.F",
    "NIKS","GREENCAR","MARRUA","EUROHOME","MICROONIBUS","BUS","CAMINHAO","CAMINHONETE",
    "ESPECIAL","ONIBUS","LO","MPOLO","MASCA","INDUSCAR","TAKO","TCA","NW"
]
# Deixe-os todos upper para matching
KNOWN_MODELS = sorted(set(m.upper() for m in KNOWN_MODELS), key=len, reverse=True)
BRANDS_OFFICIAL = sorted(set(BRANDS_OFFICIAL), key=len, reverse=True)

GENERIC_MODEL_WORDS = {
    "onibus", "ônibus", "microonibus", "micro-ônibus", "micro ônibus",
    "micro ônibus", "escolar", "ônibus escolar", "onibus escolar"
}

# Marcas/chassi (parcial; ajuste como quiser)
MARCAS_KNOWN = {
    "marcopolo","volkswagen","mercedes-benz","mercedes","scania","volvo","iveco","agrale","renault",
    "fiat","citroen","peugeot","higer","ankai","byd","hyundai"
}

# Carrocerias/linhas comuns (útil para achar modelo no título)
CARROCERIAS = {
    "paradiso","ideale","senior","volare","foz","apache","irizar","gg7","ddg7","dd","dd g7",
    "neo","neobus","busscar","comil","caio","induscar","italbus","vettura","mascarello","masca"
}

# Padrões frequentes de modelo/linha
MODEL_PATTERNS = [
    r"paradiso\s*\d{3,4}\s*(?:dd|ddg7|g7)?",
    r"ideale\s*\d{3}",
    r"senior(?:\s+g\d+)?",
    r"volare\s*[a-z]?\d{1,2}\w*",
    r"foz\s*super",
    r"apache\s*\w+",
    r"k\d{3}",                # Scania K310, K380...
    r"of\s*\d{3,4}",         # Mercedes OF 1519...
    r"\d{2}\.\d{3}",         # 15.190 etc.
    r"daily\s*\d{2,3}",      # Daily 45, 50, 55...
    r"sprinter(?:\s+\w+)?"
]

def _norm(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^0-9A-Za-z\s\-\.\_/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s.upper()

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def _slug(s: str) -> str:
    s = _norm(s).lower()
    s = s.replace("ô", "o").replace("ó","o").replace("ã","a").replace("ç","c").replace("é","e").replace("í","i").replace("á","a").replace("ú","u")
    return s

def is_generic_model(modelo: str) -> bool:
    if not modelo: return True
    sl = _slug(modelo)
    return (sl in GENERIC_MODEL_WORDS) or (sl == "")

def _find_brand(title_norm: str) -> Optional[str]:
    # 1) tenta aliases
    for alias, canon in BRAND_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", title_norm):
            return canon
    # 2) tenta marcas oficiais (maior primeiro)
    for b in BRANDS_OFFICIAL:
        if re.search(rf"\b{re.escape(b)}\b", title_norm):
            return b
    return None

def _find_model(title_norm: str) -> Optional[str]:
    # procura o modelo mais longo que aparecer (evita ambiguidade)
    for m in KNOWN_MODELS:
        if re.search(rf"\b{re.escape(m)}\b", title_norm):
            return m
    return None

def infer_marca_modelo_from_title(title: str) -> tuple[str, str]:
    if not title:
        return "", ""
    t_norm = _norm(title)

    marca = _find_brand(t_norm) or ""
    modelo = _find_model(t_norm) or ""

    if marca and not modelo:
        # fallback: pega o trecho depois da marca como modelo bruto
        m = re.search(rf"\b{re.escape(_norm(marca))}\b(.*)$", t_norm)
        if m:
            modelo = m.group(1)
            # limpa ano, km e ruídos
            modelo = re.sub(r"\b(19[5-9]\d|20[0-4]\d)\b", " ", modelo)   # anos
            modelo = re.sub(r"\b\d[\d\.\,]*\s*KM\b", " ", modelo)        # km
            modelo = re.split(r"\b(RODOVIARIO|URBANO|MICRO\s*ONIBUS|ONIBUS|EXECUTIVO|COM\s+AR|AR\s+CONDICIONADO)\b",
                              modelo, flags=re.IGNORECASE)[0]
            modelo = re.sub(r"[\-\–\|·:]+", " ", modelo)
            modelo = re.sub(r"\s+", " ", modelo).strip()

    return (marca.title().replace("-Benz","-Benz").replace("Vw","VW"), modelo.title())

def infer_model_from_title(title: str, marca: str) -> Optional[str]:
    t = _norm(title)
    if not t:
        return None
    tl = _slug(t)

    # 1) padrões conhecidos
    for pat in MODEL_PATTERNS:
        m = re.search(pat, tl, flags=re.I)
        if m:
            # retorna como aparece no título (recorta no original para manter caixa)
            start, end = m.span()
            return t[start:end].strip().upper()

    # 2) se tiver a marca no título, pega o "chunk" à frente
    marca_sl = _slug(marca or "")
    if marca_sl:
        # encontra primeira ocorrência da marca (ou marcas similares) no título
        tokens = t.split()
        tl_tokens = tl.split()
        for i, tok in enumerate(tl_tokens):
            if marca_sl in tok or (marca_sl == "mercedes-benz" and ("mercedes" in tok or "mbb" in tok)):
                # pega até 5 tokens à frente, parando em separadores comuns
                j = i + 1
                collected = []
                stop_words = {"usado","seminovo","motor","dianteiro","traseiro","mbb","vw","volkswagen","mercedes","mercedes-benz"}
                while j < len(tokens) and len(collected) < 6:
                    raw = tokens[j]
                    low = tl_tokens[j]
                    if low in stop_words or re.match(r"\d{4}$", low) or "km" in low:
                        break
                    # ignora palavras genéricas
                    if _slug(raw) in GENERIC_MODEL_WORDS:
                        j += 1
                        continue
                    collected.append(raw)
                    j += 1
                cand = " ".join(collected).strip()
                cand = re.sub(r"[·\|\-–]+.*$", "", cand).strip()  # corta depois de separadores
                if len(cand) >= 3:
                    return cand.upper()

    # 3) fallback: pega primeiro bloco com letras+números que pareça nome de linha
    m2 = re.search(r"\b([A-Z][A-Za-z]+(?:\s+[A-Z0-9][A-Za-z0-9]+){0,3})\b", t)
    if m2:
        cand = m2.group(1).strip()
        if len(cand) >= 3 and _slug(cand) not in GENERIC_MODEL_WORDS:
            return cand.upper()

    return None

def refine_marca_modelo(marca: str, modelo: str, title: str) -> Tuple[str, str]:
    m = _norm(marca)
    md = _norm(modelo)
    t = _norm(title)

    # se modelo genérico -> tentar inferir
    if is_generic_model(md):
        inferred = infer_model_from_title(t, m)
        if inferred:
            md = inferred

    # se ainda genérico, tenta pegar chassi numérico comum (15.190, OF 1519, etc.)
    if is_generic_model(md):
        mnum = re.search(r"\b(\d{2}\.\d{3}|OF\s*\d{3,4}|K\d{3})\b", t, flags=re.I)
        if mnum:
            md = mnum.group(1).upper()

    # se título mencionar carroceria (CAIO, COMIL, BUSSCAR, NEOBUS, etc.), e for diferente da marca, junta
    tl = _slug(t)
    carroceria_hit = None
    for c in CARROCERIAS:
        if re.search(rf"\b{re.escape(c)}\b", tl):
            carroceria_hit = c.upper()
            break
    if carroceria_hit and _slug(m) not in {carroceria_hit.lower(), "mascarello", "masca"}:
        # se modelo não contém já a carroceria, prefixa
        if carroceria_hit not in md:
            md = f"{carroceria_hit} {md}".strip()

    # tira 'ONIBUS' remanescente do modelo
    if _slug(md) in GENERIC_MODEL_WORDS:
        md = ""

    return (m, md)

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

                # ===== PREÇO =====
                preco_val = await get_preco_estavel(page)
                if isinstance(preco_val, (int, float)):
                    preco_txt = f"R$ {int(preco_val):,}".replace(",", ".")
                else:
                    preco_txt = await get_text(page, SELETORES_PRECO)

                # ===== MARCA / MODELO pelo TÍTULO com fallback =====
                title_txt = await get_text(page, SEL_TITLE)
                marca_h, modelo_h = infer_marca_modelo_from_title(title_txt)

                marca  = marca_h  or await get_text(page, SEL_MARCA)  or await get_text(page, XPATH_POR_LABEL["Marca"])
                modelo = modelo_h or await get_text(page, SEL_MODELO) or await get_text(page, XPATH_POR_LABEL["Modelo"])
                # >>> Refinar marca/modelo quando vier "Ônibus" etc.
                marca, modelo = refine_marca_modelo(marca, modelo, title_txt)

                # ===== ANO / KM pelo SUBTÍTULO do HEADER com fallbacks =====
                header_txt = await get_text(page, SEL_HEADER_SUB)
                ano_h, km_h = parse_ano_km_from_header(header_txt)

                ano = str(ano_h) if ano_h is not None else ""
                km  = (f"{km_h} km") if km_h is not None else ""

                if not ano:
                    ano = await get_text(page, SEL_ANO) or await get_text(page, XPATH_POR_LABEL["Ano"])
                if not km:
                    km  = await get_text(page, SEL_KM)  or await get_text(page, XPATH_POR_LABEL["Km"])

                # ===== NORMALIZAÇÕES NUMÉRICAS =====
                def parse_int_from(txt: str) -> Optional[int]:
                    if not txt:
                        return None
                    m = re.search(r"\d{4}", txt)
                    if m:
                        try:
                            return int(m.group(0))
                        except:
                            return None
                    return None

                def parse_km(txt: str) -> Optional[int]:
                    if not txt:
                        return None
                    s = txt.lower().replace("km", "")
                    s = re.sub(r"[^\d]", "", s)
                    if not s:
                        return None
                    try:
                        return int(s)
                    except:
                        return None

                ano_val = parse_int_from(ano)
                km_val  = parse_km(km)

                dados = {
                    "Link": link,
                    "Preço_txt": preco_txt,
                    "Preço": preco_val if preco_val is not None else "",
                    "Marca": marca,
                    "Modelo": modelo,
                    "Ano_txt": ano,
                    "Ano": ano_val if ano_val is not None else "",
                    "Km_txt": km,
                    "Km": km_val if km_val is not None else "",
                }

                return dados

            except Exception as e:
                if tent >= RETRIES_PAGE:
                    logging.warning(f"[{link}] erro final: {e}")
                    return None
                await asyncio.sleep(0.8 * tent)
        return None
    finally:
        try:
            await page.close()
        except:
            pass

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

    # checkpoint (retomar onde parou)
    if os.path.exists(ARQUIVO_CHECKPOINT):
        try:
            dados_coletados = pd.read_pickle(ARQUIVO_CHECKPOINT).to_dict("records")
            processados = {d["Link"] for d in dados_coletados if "Link" in d}
            links = [l for l in links if l not in processados]
            logging.info(f"Checkpoint: {len(dados_coletados)} já salvos, {len(links)} restantes.")
        except Exception as e:
            logging.error(f"Erro ao carregar checkpoint: {e}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=80)  
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

        # Usa a sessão salva, se habilitado
        if USE_LOGIN:
            if not os.path.exists(STORAGE_STATE):
                raise FileNotFoundError(
                    f"{STORAGE_STATE} não encontrado. Gere a sessão com --salvar-estado."
                )
            context_kwargs["storage_state"] = STORAGE_STATE

        context = await browser.new_context(**context_kwargs)

        context.set_default_timeout(TIMEOUT_MS)

        sem = asyncio.Semaphore(max_concurrent)

        async def worker(url: str):
            async with sem:
                return await extrair_anuncio(context, url)

        # Processa com progresso
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

    # Salva PKL (completo do run)
    await asyncio.to_thread(df.to_pickle, ARQUIVO_PKL_DADOS)
    logging.info(f"PKL salvo: {ARQUIVO_PKL_DADOS}")

    # Consolida/Anexa no Excel final
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