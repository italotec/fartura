#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import json
import random
import string
import uuid
import time
from datetime import datetime
import sys

BM_FILE = 'bms.json'
LOG_FILE = 'sent_log.csv'
TEMPLATE_LANG = 'pt_BR'
LOCK = threading.Lock()

# Proxy do Tor (Tails usa porta 9050 por padrão)
TOR_PROXY = {
    "http": "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# === Geradores aleatórios ===
def random_namespace():
    u = str(uuid.uuid4())
    parts = u.split('-')
    return f"{parts[0]}*{parts[1]}*{parts[2]}*{parts[3]}*{parts[4]}"

def random_parameter_name(length=6):
    return random.choice(string.ascii_lowercase) + ''.join(random.choices(string.ascii_lowercase + string.digits, k=length-1))

NAMESPACE_VALUE = random_namespace()
PARAM_NAME_VALUE = random_parameter_name()

def carregar_bms():
    if not os.path.exists(BM_FILE):
        return {}
    with open(BM_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def salvar_bms(bms):
    with open(BM_FILE, 'w', encoding='utf-8') as f:
        json.dump(bms, f, indent=4, ensure_ascii=False)

def cadastrar_bm():
    bms = carregar_bms()
    nome = input("Nome da BM: ").strip()
    phone_number_id = input("Phone Number ID: ").strip()
    token = input("Token: ").strip()
    templates_raw = input("Templates (separados por vírgula): ").strip()
    templates = [t.strip() for t in templates_raw.split(',') if t.strip()]
    bms[nome] = {
        "phone_number_id": phone_number_id,
        "token": token,
        "templates": templates
    }
    salvar_bms(bms)
    print(f"✅ BM '{nome}' cadastrada com sucesso.")

# Controles globais para pausa/resume/quit
pause_event = threading.Event()
pause_event.set()  # set => envia; clear => pausa
stop_event = threading.Event()

def ouvinte_controle():
    """
    Thread que escuta comandos do usuário:
      p -> pausar
      r -> resumir
      q -> sair
    """
    print("\nTeclas de controle: [p]ausar, [r]esumir, [q]sair")
    while not stop_event.is_set():
        try:
            cmd = input().strip().lower()
        except EOFError:
            break
        if not cmd:
            continue
        if cmd == 'p':
            if pause_event.is_set():
                pause_event.clear()
                print("[pausado]")
            else:
                print("[já está pausado]")
        elif cmd == 'r':
            if not pause_event.is_set():
                pause_event.set()
                print("[retomado]")
            else:
                print("[já está em execução]")
        elif cmd == 'q':
            print("[solicitação de saída recebida]")
            stop_event.set()
            pause_event.set()  # libera casos pausados para terminar
            break
        else:
            print("Comando desconhecido. Use p (pausar), r (resumir), q (sair).")

def log_result(phone, status, details=""):
    ts = datetime.utcnow().isoformat()
    with LOCK:
        header_needed = not os.path.exists(LOG_FILE) or os.path.getsize(LOG_FILE) == 0
        with open(LOG_FILE, "a", encoding='utf-8') as f:
            if header_needed:
                f.write("phone,status,details,timestamp\n")
            safe_details = str(details).replace("\n"," ").replace(",", ";")
            f.write(f"{phone},{status},{safe_details},{ts}\n")

def montar_componentes_por_mapeamento(linha_lead, mapeamento):
    """
    mapeamento: lista de tuplas (nome_coluna, nome_variavel)
    Retorna a lista 'components' para o payload do template
    """
    params = []
    for col, varname in mapeamento:
        text_value = str(linha_lead.get(col, '')).strip()
        params.append({"type": "text", "parameter_name": varname, "text": text_value})
    return [
        {
            "type": "body",
            "parameters": params
        }
    ]

def enviar_template(lead, phone_number_id, token, mapeamento, log_enabled=True):
    """
    lead: Series ou dict-like
    mapeamento: lista de (coluna, nome_variavel)
    """
    # Bloqueia enquanto estiver pausado
    pause_event.wait()
    if stop_event.is_set():
        return

    telefone = str(lead.get('telefone', '')).strip()
    if not telefone:
        print(f"⚠️ Lead sem telefone: {lead}")
        if log_enabled:
            log_result("", "skipped", "no phone")
        return

    template_name = str(lead.get('template_name', '')).strip()
    if not template_name:
        print(f"⚠️ Lead sem template_name: {lead}")
        if log_enabled:
            log_result(telefone, "skipped", "no template_name")
        return

    api_url = f"https://graph.facebook.com/v23.0/{phone_number_id}/messages"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    components = montar_componentes_por_mapeamento(lead, mapeamento)

    payload = {
        "type": "template",
        "messaging_product": "whatsapp",
        "template": {
            "namespace": NAMESPACE_VALUE,
            "name": template_name,
            "language": {"code": TEMPLATE_LANG},
            "components": components
        },
        "to": telefone
    }

    try:
        response = requests.post(api_url, headers=headers, json=payload, proxies=TOR_PROXY, timeout=30)
        status_code = getattr(response, "status_code", "ERR")
        text = getattr(response, "text", "")
        print(f"{telefone}: {status_code} | {text} | namespace={NAMESPACE_VALUE}")
        if response.status_code == 200:
            log_result(telefone, "delivered", text if log_enabled else "")
        else:
            log_result(telefone, f"error_{status_code}", text)
    except Exception as e:
        print(f"Erro ao enviar para {telefone}: {e}")
        log_result(telefone, "exception", str(e))

def modo_envio_interativo(random_mode=False, csv_path=None):
    bms = carregar_bms()
    if not bms:
        print("❌ Nenhuma BM cadastrada. Use a opção '1' do menu para cadastrar uma.")
        return

    print("\nBMs disponíveis:")
    for i, nome in enumerate(bms.keys()):
        print(f"{i + 1}. {nome}")
    escolha = input("Escolha o número da BM que deseja usar: ").strip()
    try:
        index = int(escolha) - 1
        bm_nome = list(bms.keys())[index]
    except (ValueError, IndexError):
        print("❌ Escolha inválida.")
        return

    bm = bms[bm_nome]
    phone_number_id = bm['phone_number_id']
    token = bm['token']
    templates = bm.get('templates', [])

    if not csv_path:
        csv_path = input("Informe o caminho/nome do arquivo CSV de leads: ").strip()
    if not csv_path:
        print("❌ Nenhum CSV informado. Abortando.")
        return

    if not os.path.exists(csv_path):
        print(f"❌ Arquivo CSV não encontrado em '{csv_path}'. Verifique o caminho.")
        return

    # Ler CSV como string para evitar .0 em telefones
    try:
        leads = pd.read_csv(csv_path, dtype=str).fillna('')
    except Exception as e:
        print(f"Erro ao ler CSV: {e}")
        return

    # Exibir cabeçalhos e permitir mapeamento (automático: parameter_name = nome da coluna)
    headers = list(leads.columns)
    print("\nCabeçalhos detectados no CSV:")
    for i, h in enumerate(headers):
        print(f"  {i + 1}. {h}")

    print("\nAgora selecione os cabeçalhos NA ORDEM que quer mapear para variáveis do template.")
    print("Ao selecionar uma coluna pelo número, o parameter_name será automaticamente o NOME do cabeçalho.")
    print("Pressione Enter em um input vazio para finalizar.\n")

    mapeamento = []  # lista de tuplas (coluna, nome_variavel)
    indices_escolhidos = set()
    while True:
        choice = input("Número do cabeçalho (vazio para terminar): ").strip()
        if choice == "":
            break
        try:
            idx = int(choice) - 1
            if idx < 0 or idx >= len(headers):
                print("Número inválido.")
                continue
            if idx in indices_escolhidos:
                print("Cabeçalho já selecionado.")
                continue
            col_name = headers[idx]
            # Atribuição automática: parameter_name = nome da coluna (exatamente como no cabeçalho)
            var_name = col_name
            mapeamento.append((col_name, var_name))
            indices_escolhidos.add(idx)
            print(f"Adicionado mapeamento: coluna '{col_name}' -> parameter_name '{var_name}'")
        except ValueError:
            print("Digite um número válido.")

    if not mapeamento:
        print("Nenhuma coluna mapeada. Abortando.")
        return

    # Velocidade (max_workers)
    while True:
        velocity = input("Informe a velocidade de envio como inteiro (max_workers, padrão 1): ").strip()
        if velocity == "":
            max_workers = 1
            break
        try:
            max_workers = int(velocity)
            if max_workers < 1:
                print("Deve ser >= 1")
                continue
            break
        except ValueError:
            print("Digite um inteiro válido.")

    # Preparar log
    if not os.path.exists(LOG_FILE):
        open(LOG_FILE, "w", encoding='utf-8').close()

    with open(LOG_FILE, "r", encoding='utf-8') as f:
        enviados = set(line.split(",")[0].strip() for line in f if line.strip() and not line.startswith("phone,"))

    # Filtrar leads já enviados (se houver coluna 'telefone')
    if 'telefone' in leads.columns:
        leads_filtrados = leads[~leads['telefone'].astype(str).isin(enviados)].reset_index(drop=True)
    else:
        leads_filtrados = leads.copy()

    if random_mode:
        leads_filtrados = leads_filtrados.sample(frac=1).reset_index(drop=True)

    # Rotacionar templates se existirem
    num_templates = len(templates)
    total_leads = len(leads_filtrados)
    if num_templates > 0:
        leads_filtrados['template_name'] = [templates[i % num_templates] for i in range(total_leads)]
    else:
        leads_filtrados['template_name'] = [''] * total_leads  # deve existir template_name no CSV

    print(f"\n📤 Iniciando envio para {total_leads} leads...")
    print(f"📌 Usando namespace: {NAMESPACE_VALUE}")

    # iniciar thread do ouvinte de controle
    ctl_thread = threading.Thread(target=ouvinte_controle, daemon=True)
    ctl_thread.start()

    # Executor de threads com max_workers escolhido
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for _, lead in leads_filtrados.iterrows():
            if stop_event.is_set():
                break
            lead_dict = lead.to_dict()
            futures.append(executor.submit(enviar_template, lead_dict, phone_number_id, token, mapeamento, log_enabled=not random_mode))
            time.sleep(0.01)
        try:
            for fut in as_completed(futures):
                if stop_event.is_set():
                    break
                try:
                    fut.result()
                except Exception as e:
                    print("Exceção em worker:", e)
        except KeyboardInterrupt:
            print("KeyboardInterrupt recebido. Parando...")
            stop_event.set()
        finally:
            stop_event.set()
            pause_event.set()
            print("Envio finalizado ou interrompido.")

def menu_principal():
    while True:
        print("\n=== MENU ===")
        print("1) Cadastrar nova BM")
        print("2) Enviar (usar CSV de leads)")
        print("3) Sair")
        escolha = input("Escolha uma opção (1/2/3): ").strip()
        if escolha == '1':
            cadastrar_bm()
        elif escolha == '2':
            modo_envio_interativo(random_mode=False, csv_path=None)
        elif escolha == '3':
            print("Saindo.")
            break
        else:
            print("Opção inválida. Digite 1, 2 ou 3.")

if __name__ == "__main__":
    try:
        menu_principal()
    except Exception as e:
        print("Erro inesperado:", e)
        sys.exit(1)
