from playwright.async_api import async_playwright
import asyncio, os, sys

# === CONFIG ===
USE_LOGIN = True
STORAGE_STATE = r"C:\Users\gabriel.vinicius\Documents\Vscode\MicroOnibus\ml_state.json"
PAGE_TIMEOUT_MS = 90_000

# --- salvar sessão (LOGIN MANUAL) ---
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
        print("\nFaça login manualmente nessa janela.")
        await asyncio.to_thread(input, "Quando terminar, aperte ENTER aqui para salvar a sessão… ")
        await context.storage_state(path=STORAGE_STATE)
        await browser.close()
        print(f"✅ Estado salvo em: {STORAGE_STATE}")

# --- criar contexto já usando a sessão ---
async def criar_contexto(p):
    context_kwargs = dict(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1366, "height": 860},
        locale="pt-BR",
        timezone_id="America/Sao_Paulo",
        extra_http_headers={"Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"},
    )
    if USE_LOGIN:
        if not os.path.exists(STORAGE_STATE):
            print(f"[INFO] Sessão não encontrada: {STORAGE_STATE}")
            await salvar_estado_login()
        context_kwargs["storage_state"] = STORAGE_STATE

    browser = await p.chromium.launch(headless=False)  # no collector mantenha headless=False para depurar
    context = await browser.new_context(**context_kwargs)
    context.set_default_timeout(PAGE_TIMEOUT_MS)
    return browser, context

# ---------------- main ----------------
async def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--salvar-estado":
        await salvar_estado_login()
        return

    async with async_playwright() as p:
        browser, context = await criar_contexto(p)
        # >>> seu código de coleta de links aqui (abrir_pagina_busca, ir_para_proxima, etc.)
        await context.close()
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
