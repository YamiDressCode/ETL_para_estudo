# main.py - ETL UNIPIX ULTIMATE COM API + TOKENS (SSL FIX)
import os
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import logging
import shutil
import zipfile
import tempfile
import getpass
import time
import json
import re
import calendar
from pathlib import Path
from datetime import datetime, date, time as dtime
import urllib3
import ssl

import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Desabilitar warnings de SSL (opcional)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =============================================================================
# CONFIGURAÇÕES GLOBAIS 
# =============================================================================
UNIPIX_USUARIO = "marlon.carvalho@mds.gov.br"
UNIPIX_SENHA = "Mds@2025"
DOWNLOAD_FOLDER = r"C:\Users\marlon.carvalho\Desktop\aprendizado\data\input"

# URLs da API
LOGIN_URL = "https://avia.unipix.com.br/#/login"
API_URL = "https://aws-api-sms-interna.unipix.com.br/relatorio-analitico"
DEFAULT_PAGE_SIZE = 500

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
class Config:
    def __init__(self):
        # Pastas
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.input_folder = DOWNLOAD_FOLDER
        self.processed_folder = os.path.join(self.base_dir, 'data', 'processed') 
        self.error_folder = os.path.join(self.base_dir, 'data', 'error')
        self.temp_folder = os.path.join(self.base_dir, 'data', 'temp')
        
        # Criar pastas
        for folder in [self.input_folder, self.processed_folder, self.error_folder, self.temp_folder]:
            os.makedirs(folder, exist_ok=True)
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger('ETL-API')

# =============================================================================
# LEITOR DE CÓDIGO DA PLANILHA EXCEL (MANTIDO IGUAL)
# =============================================================================
class PlanilhaCodeReader:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger
        self.caminho_planilha = r"C:\Users\marlon.carvalho\OneDrive - Ministério do Desenvolvimento e Assistência Social\Documentos\Unip\cod_unipix.csv"
    
    def aguardar_planilha_pronta(self, tempo_maximo=180):
        """Aguarda a planilha ficar disponível e estável"""
        try:
            self.logger.info(f"⏳ Aguardando planilha ficar pronta (máximo {tempo_maximo}s)...")
            
            tempo_inicio = time.time()
            ultimo_tamanho = 0
            tentativas_estavel = 0
            
            while time.time() - tempo_inicio < tempo_maximo:
                if not os.path.exists(self.caminho_planilha):
                    self.logger.info("📁 Planilha ainda não encontrada, aguardando...")
                    time.sleep(5)
                    continue
                
                try:
                    tamanho_atual = os.path.getsize(self.caminho_planilha)
                    
                    if tamanho_atual != ultimo_tamanho:
                        self.logger.info(f"📊 Planilha detectada, tamanho: {tamanho_atual} bytes")
                        ultimo_tamanho = tamanho_atual
                        tentativas_estavel = 0
                        time.sleep(2)
                        continue
                    
                    tentativas_estavel += 1
                    if tentativas_estavel >= 3:
                        self.logger.info("✅ Planilha estável e pronta para leitura")
                        return True
                    
                    time.sleep(2)
                    
                except OSError as e:
                    self.logger.info(f"⚠️  Arquivo ainda não acessível: {e}")
                    time.sleep(3)
                    continue
            
            self.logger.error("❌ Timeout - Planilha não ficou pronta a tempo")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao aguardar planilha: {e}")
            return False
    
    def ler_codigo_da_planilha(self):
        """Lê o código da célula 1A da planilha CSV"""
        try:
            if not self.aguardar_planilha_pronta():
                return None
            
            self.logger.info(f"📊 Lendo planilha CSV: {self.caminho_planilha}")
            
            for tentativa in range(5):
                try:
                    try:
                        df = pd.read_csv(self.caminho_planilha, header=None, encoding='utf-8')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(self.caminho_planilha, header=None, encoding='latin-1')
                        except UnicodeDecodeError:
                            df = pd.read_csv(self.caminho_planilha, header=None, encoding='cp1252')
                    
                    if df.empty:
                        self.logger.warning(f"⚠️  Planilha vazia na tentativa {tentativa + 1}")
                        time.sleep(3)
                        continue
                    
                    codigo = df.iloc[0, 0] if not df.empty else None
                    
                    if pd.isna(codigo) or codigo == "" or str(codigo).strip() == "":
                        self.logger.warning(f"⚠️  Célula A1 vazia na tentativa {tentativa + 1}")
                        time.sleep(3)
                        continue
                    
                    codigo_str = str(codigo).strip()
                    self.logger.info(f"✅ Código lido da planilha: {codigo_str}")
                    
                    if self.validar_formato_codigo(codigo_str):
                        return codigo_str
                    else:
                        self.logger.warning(f"⚠️  Código com formato inválido: {codigo_str}")
                        time.sleep(3)
                        continue
                        
                except Exception as e:
                    self.logger.warning(f"⚠️  Erro na tentativa {tentativa + 1}: {e}")
                    time.sleep(3)
                    continue
            
            self.logger.error("❌ Todas as tentativas de leitura falharam")
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao ler planilha: {e}")
            return None
    
    def validar_formato_codigo(self, codigo):
        """Valida se o código está no formato correto"""
        import re
        
        padroes_validos = [
            r'^[A-Za-z0-9]{3}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$',
            r'^[A-Za-z0-9]{6,9}$',
            r'^\d{6}$',
        ]
        
        for padrao in padroes_validos:
            if re.match(padrao, codigo):
                return True
        
        self.logger.warning(f"⚠️  Formato de código não reconhecido: {codigo}")
        return False

# =============================================================================
# UTILITÁRIOS PARA API
# =============================================================================
def is_jwt(texto: str) -> bool:
    """Verifica se uma string é um JWT válido"""
    if not isinstance(texto, str):
        return False
    partes = texto.split('.')
    return len(partes) == 3

def first_last_of_current_month_utc_isoz(tz_name: str = "America/Sao_Paulo"):
    """Retorna datas do mês atual em formato ISO UTC"""
    today = date.today()
    ano, mes = today.year, today.month
    primeiro_dia = date(ano, mes, 1)
    ultimo_dia = date(ano, mes, calendar.monthrange(ano, mes)[1])

    inicio_iso = f"{primeiro_dia.strftime('%Y-%m-%d')}T00:00:00.000Z"
    fim_iso = f"{ultimo_dia.strftime('%Y-%m-%d')}T23:59:59.000Z"
    yyyymm = f"{ano}{mes:02d}"
    
    return inicio_iso, fim_iso, yyyymm

def build_params(inicio_iso: str, fim_iso: str, page: int, size: int, **extras) -> dict:
    """Constrói parâmetros para a API"""
    base = {
        "page": page,
        "size": size,
        "campanha": "",
        "produto": "",
        "mensagem": "",
        "smsClienteId": "",
        "via": "",
        "cliente": "",
        "centroCusto": "",
        "usuario": "",
        "higienizacao": "",
        "status": "",
        "tarifado": "",
        "contato": "",
        "dataFinalEnvio": fim_iso,
        "dataInicialEnvio": inicio_iso,
        "dataFinalAgendamento": "",
        "dataInicialAgendamento": "",
        "dataFinalStatus": "",
        "dataInicialStatus": ""
    }
    base.update({k: v for k, v in extras.items() if v is not None})
    return base

def cookies_selenium_para_requests(cookies_selenium, target_domain: str):
    """Converte cookies do Selenium para formato do requests"""
    jar = requests.cookies.RequestsCookieJar()
    base_domain = ".unipix.com.br"
    for c in cookies_selenium:
        name = c.get("name")
        value = c.get("value")
        domain = c.get("domain") or base_domain
        if domain.startswith("avia.") or domain == "unipix.com.br":
            domain = base_domain
        path = c.get("path") or "/"
        jar.set(name, value, domain=domain, path=path)
    return jar

# =============================================================================
# UNIPIX SCRAPER COM API - VERSÃO ALTERNATIVA (SSL FIXED)
# =============================================================================
class UnipixScraperAPI:
    def __init__(self, config, download_folder):
        self.config = config
        self.download_folder = download_folder
        self.driver = None
        self.wait = None
        self.logger = config.logger
        self.planilha_reader = PlanilhaCodeReader(config)
        self.token = None
        self.cookies = None
    
    def configurar_chrome(self, headless=False):
        """Configura o Chrome para autenticação"""
        chrome_options = Options()
        
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 30)
        self.logger.info("✅ Navegador Chrome configurado")
    
    def coletar_credenciais_usuario(self):
        """Coleta credenciais e período do usuário"""
        print("\n" + "="*70)
        print("🏢 UNIPIX - CONFIGURAÇÃO AUTOMÁTICA (API VERSION)")
        print("="*70)
        
        credenciais = {}
        
        # USA CREDENCIAIS PRÉ-DEFINIDAS
        credenciais['usuario'] = UNIPIX_USUARIO
        credenciais['senha'] = UNIPIX_SENHA
        
        print(f"👤 Usuário: {credenciais['usuario']}")
        print("🔒 Senha: **********")
        
        # PERÍODO DO RELATÓRIO
        print("\n📅 PERÍODO DO RELATÓRIO ANALÍTICO")
        print("💡 Formato: DD/MM/AAAA - DD/MM/AAAA")
        periodo = input("📅 Digite o período (ex: 03/10/2024 - 17/10/2024): ").strip()
        
        if not periodo or ' - ' not in periodo:
            print("❌ Período no formato inválido! Use: DD/MM/AAAA - DD/MM/AAAA")
            return None
        
        credenciais['periodo'] = periodo
        
        return credenciais
    
    def fazer_login_unipix(self, usuario, senha):
        """Faz login no site da Unipix e extrai token/cookies"""
        try:
            self.logger.info("🔐 Fazendo login na Unipix via API...")
            print("🔐 Realizando login...")
            
            # Configura navegador
            self.configurar_chrome(headless=False)
            
            # Acessa a página de login
            self.driver.get(LOGIN_URL)
            time.sleep(5)
            
            # Aguarda a página carregar
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Preenche usuário
            seletores_usuario = [
                "//input[@name='username']",
                "//input[@id='username']",
                "//input[@placeholder='Usuário']",
                "//input[@type='text']",
                "//input[@type='email']"
            ]
            
            campo_usuario = None
            for seletor in seletores_usuario:
                try:
                    campo_usuario = self.driver.find_element(By.XPATH, seletor)
                    break
                except:
                    continue
            
            if not campo_usuario:
                self.logger.error("❌ Campo de usuário não encontrado")
                return False
            
            campo_usuario.clear()
            campo_usuario.send_keys(usuario)
            self.logger.info("✅ Usuário preenchido")
            
            # Preenche senha
            seletores_senha = [
                "//input[@name='password']",
                "//input[@id='password']", 
                "//input[@placeholder='Senha']",
                "//input[@type='password']"
            ]
            
            campo_senha = None
            for seletor in seletores_senha:
                try:
                    campo_senha = self.driver.find_element(By.XPATH, seletor)
                    break
                except:
                    continue
            
            if not campo_senha:
                self.logger.error("❌ Campo de senha não encontrado")
                return False
            
            campo_senha.clear()
            campo_senha.send_keys(senha)
            self.logger.info("✅ Senha preenchida")
            
            # Clica no botão de login
            seletores_botao = [
                "//button[@type='submit']",
                "//button[contains(text(), 'Login')]",
                "//button[contains(text(), 'Entrar')]",
                "//input[@type='submit']"
            ]
            
            botao_login = None
            for seletor in seletores_botao:
                try:
                    botao_login = self.driver.find_element(By.XPATH, seletor)
                    break
                except:
                    continue
            
            if not botao_login:
                self.logger.error("❌ Botão de login não encontrado")
                return False
            
            botao_login.click()
            self.logger.info("✅ Botão de login clicado")
            
            # Aguarda o processamento do login
            time.sleep(5)
            
            # Verifica se precisa de autenticação de dois fatores
            if self._verificar_se_precisa_2fa():
                return self._processar_autenticacao_2fa_automatica()
            else:
                # Login sem 2FA
                time.sleep(5)
                if self._verificar_login_sucesso():
                    return self._extrair_token_cookies()
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erro no login: {e}")
            print(f"❌ Erro no login: {e}")
            return False
    
    def _verificar_se_precisa_2fa(self):
        """Verifica se a página pede autenticação de dois fatores"""
        try:
            indicadores_2fa = [
                "//*[contains(text(), 'dois fatores')]",
                "//*[contains(text(), '2FA')]",
                "//*[contains(text(), 'autenticação')]",
                "//*[contains(text(), 'código')]",
                "//input[@name='code']",
                "//input[@placeholder='Código']",
                "//input[@type='number']"
            ]
            
            for indicador in indicadores_2fa:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        self.logger.info("🔐 Autenticação de dois fatores detectada")
                        print("🔐 Autenticação de dois fatores necessária")
                        return True
                except:
                    continue
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao verificar 2FA: {e}")
            return False
    
    def _processar_autenticacao_2fa_automatica(self):
        """Processa autenticação 2FA usando código da planilha"""
        try:
            print("\n📱 AUTENTICAÇÃO VIA PLANILHA EXCEL")
            print("="*50)
            print("⏳ Aguardando planilha ser atualizada...")
            
            time.sleep(60)
            
            # Obtém código da planilha
            codigo_2fa = self.obter_codigo_verificacao()
            
            if not codigo_2fa:
                print("❌ Não foi possível obter código da planilha")
                return self._processar_autenticacao_2fa_manual()
            
            print(f"✅ Código obtido da planilha: {codigo_2fa}")
            
            # Preenche o código no site
            return self._preencher_codigo_2fa_no_site(codigo_2fa)
            
        except Exception as e:
            self.logger.error(f"❌ Erro na autenticação automática: {e}")
            return self._processar_autenticacao_2fa_manual()
    
    def obter_codigo_verificacao(self, tempo_espera=60):
        """Obtém o código de verificação da planilha"""
        try:
            self.logger.info("📊 Obtendo código da planilha...")
            print(f"⏳ Aguardando {tempo_espera} segundos para planilha atualizar...")
            
            for i in range(tempo_espera // 10, 0, -1):
                print(f"   🕒 {i * 10} segundos restantes...")
                time.sleep(10)
            
            self.logger.info("✅ Finalizou espera, lendo planilha...")
            
            codigo = self.planilha_reader.ler_codigo_da_planilha()
            
            if codigo and self.planilha_reader.validar_formato_codigo(codigo):
                self.logger.info(f"✅ Código validado: {codigo}")
                return codigo
            else:
                self.logger.error("❌ Código inválido ou não encontrado na planilha")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao obter código da planilha: {e}")
            return None
    
    def _preencher_codigo_2fa_no_site(self, codigo_2fa):
        """Preenche o código 2FA no site"""
        try:
            # Procura pelo campo do código 2FA
            seletores_codigo = [
                "//input[@name='code']",
                "//input[@placeholder='Código']", 
                "//input[@type='number']",
                "//input[contains(@id, 'code')]",
                "//input[contains(@name, 'token')]",
                "//input[@type='text']"
            ]
            
            campo_codigo = None
            for seletor in seletores_codigo:
                try:
                    campo_codigo = self.driver.find_element(By.XPATH, seletor)
                    break
                except:
                    continue
            
            if not campo_codigo:
                print("❌ Campo do código de autenticação não encontrado")
                return False
            
            # Preenche o código
            campo_codigo.clear()
            campo_codigo.send_keys(codigo_2fa)
            self.logger.info("✅ Código 2FA preenchido")
            
            # Procura e clica no botão de verificação
            seletores_verificar = [
                "//button[contains(text(), 'Verificar')]",
                "//button[contains(text(), 'Confirmar')]",
                "//button[@type='submit']",
                "//button[contains(text(), 'Enviar')]"
            ]
            
            botao_verificar = None
            for seletor in seletores_verificar:
                try:
                    botao_verificar = self.driver.find_element(By.XPATH, seletor)
                    botao_verificar.click()
                    break
                except:
                    continue
            
            if not botao_verificar:
                print("❌ Botão de verificação não encontrado")
                return False
            
            self.logger.info("✅ Botão de verificação clicado")
            print("⏳ Verificando código...")
            
            # Aguarda a verificação
            time.sleep(8)
            
            if self._verificar_login_sucesso():
                return self._extrair_token_cookies()
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao preencher código: {e}")
            return False
    
    def _processar_autenticacao_2fa_manual(self):
        """Fallback para autenticação manual"""
        try:
            print("\n🔢 AUTENTICAÇÃO MANUAL")
            codigo_2fa = input("🔢 Digite o código de autenticação de dois fatores: ").strip()
            
            if not codigo_2fa:
                print("❌ Código de autenticação é obrigatório!")
                return False
            
            return self._preencher_codigo_2fa_no_site(codigo_2fa)
            
        except Exception as e:
            self.logger.error(f"❌ Erro na autenticação manual: {e}")
            return False
    
    def _verificar_login_sucesso(self):
        """Verifica se o login foi bem-sucedido"""
        try:
            current_url = self.driver.current_url.lower()
            
            if "login" in current_url or "auth" in current_url:
                self.logger.error("❌ Login falhou - ainda na página de autenticação")
                print("❌ Login falhou! Verifique suas credenciais.")
                return False
            else:
                self.logger.info("✅ Login realizado com sucesso")
                print("✅ Login realizado com sucesso!")
                return True
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao verificar login: {e}")
            return False
    
    def _extrair_token_cookies(self):
        """Extrai token JWT e cookies após login bem-sucedido"""
        try:
            self.logger.info("🔍 Extraindo token e cookies...")
            
            # Extrai token do localStorage
            entries = self.driver.execute_script("return Object.entries(window.localStorage);")
            token = None
            
            for k, v in entries:
                if isinstance(v, str) and is_jwt(v):
                    token = v
                    break
                try:
                    obj = json.loads(v)
                    for cand in ["token", "access_token", "jwt", "auth", "authorization"]:
                        if isinstance(obj, dict) and cand in obj and is_jwt(str(obj[cand])):
                            token = str(obj[cand])
                            break
                    if token:
                        break
                except Exception:
                    pass
            
            # Se não encontrou no localStorage, tenta sessionStorage
            if not token:
                entries_s = self.driver.execute_script("return Object.entries(window.sessionStorage);")
                for k, v in entries_s:
                    if isinstance(v, str) and is_jwt(v):
                        token = v
                        break
                    try:
                        obj = json.loads(v)
                        for cand in ["token", "access_token", "jwt", "auth", "authorization"]:
                            if isinstance(obj, dict) and cand in obj and is_jwt(str(obj[cand])):
                                token = str(obj[cand])
                                break
                        if token:
                            break
                    except Exception:
                        pass
            
            # Extrai cookies
            cookies = self.driver.get_cookies()
            
            if token:
                self.token = token
                self.logger.info("✅ Token JWT extraído com sucesso")
            else:
                self.logger.warning("⚠️  Token JWT não encontrado, usando apenas cookies")
            
            if cookies:
                self.cookies = cookies
                self.logger.info(f"✅ {len(cookies)} cookies extraídos")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao extrair token/cookies: {e}")
            return False
    
    def converter_periodo_para_iso(self, periodo):
        """Converte período no formato DD/MM/AAAA - DD/MM/AAAA para ISO UTC"""
        try:
            partes = periodo.split(' - ')
            if len(partes) != 2:
                raise ValueError("Formato de período inválido")
            
            data_inicio = partes[0].strip()
            data_fim = partes[1].strip()
            
            # Converte DD/MM/AAAA para AAAA-MM-DD
            dia_i, mes_i, ano_i = data_inicio.split('/')
            dia_f, mes_f, ano_f = data_fim.split('/')
            
            inicio_iso = f"{ano_i}-{mes_i}-{dia_i}T00:00:00.000Z"
            fim_iso = f"{ano_f}-{mes_f}-{dia_f}T23:59:59.000Z"
            
            return inicio_iso, fim_iso
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao converter período: {e}")
            return None, None
    
    def baixar_relatorio_via_api(self, periodo):
        """Baixa relatório analítico via API usando token/cookies - COM SSL FIX"""
        try:
            self.logger.info("📊 Iniciando download via API...")
            print("📊 Baixando relatório via API...")
            
            # Converte período para formato ISO
            inicio_iso, fim_iso = self.converter_periodo_para_iso(periodo)
            if not inicio_iso or not fim_iso:
                return None
            
            # Prepara sessão requests COM SSL VERIFY FALSE
            sess = requests.Session()
            
            # 🔧 SOLUÇÃO SSL: Desativa verificação de certificado
            sess.verify = False
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://avia.unipix.com.br",
                "Referer": "https://avia.unipix.com.br/"
            }
            
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
                self.logger.info("🔑 Usando token JWT para autenticação")
            else:
                self.logger.info("🍪 Usando cookies para autenticação")
            
            if self.cookies:
                jar = cookies_selenium_para_requests(self.cookies, 
                                                   target_domain="aws-api-sms-interna.unipix.com.br")
                sess.cookies = jar
            
            # Faz chamadas paginadas para a API
            all_rows = []
            page = 0
            max_pages = 50  # Limite de segurança
            
            while page < max_pages:
                params = build_params(inicio_iso, fim_iso, page=page, size=DEFAULT_PAGE_SIZE)
                
                self.logger.info(f"📄 Buscando página {page + 1}...")
                
                try:
                    # 🔧 SOLUÇÃO SSL: verify=False na requisição também
                    resp = sess.get(API_URL, headers=headers, params=params, timeout=180, verify=False)
                    
                    if resp.status_code == 401:
                        self.logger.error("❌ 401 Não autorizado na API. Token/cookies inválidos.")
                        break
                    if resp.status_code >= 400:
                        self.logger.error(f"❌ Falha na API ({resp.status_code}): {resp.text[:300]}")
                        break
                    
                    # Processa resposta
                    ctype = resp.headers.get("Content-Type", "")
                    if "application/json" in ctype:
                        data = resp.json()
                        
                        if isinstance(data, dict) and "content" in data:
                            rows = data["content"] or []
                            all_rows.extend(rows)
                            
                            self.logger.info(f"📊 Página {page + 1}: {len(rows)} registros")
                            
                            # Verifica se é a última página
                            last_flag = data.get("last")
                            total_pages = data.get("totalPages")
                            total_elements = data.get("totalElements")
                            
                            if total_elements:
                                self.logger.info(f"📈 Total de registros: {total_elements}")
                            
                            if last_flag is True:
                                self.logger.info("✅ Última página alcançada")
                                break
                            if total_pages is not None and page + 1 >= int(total_pages):
                                self.logger.info("✅ Todas as páginas processadas")
                                break
                            if len(rows) < DEFAULT_PAGE_SIZE:
                                self.logger.info("✅ Fim dos dados (página incompleta)")
                                break
                        else:
                            # Outros formatos de resposta
                            rows = data.get("items") or data.get("rows") or []
                            all_rows.extend(rows)
                            self.logger.info(f"📊 Formato alternativo: {len(rows)} registros")
                            if len(rows) < DEFAULT_PAGE_SIZE:
                                break
                    else:
                        # Se não for JSON, trata como CSV
                        self.logger.info("📄 Resposta em formato CSV detectada")
                        df = pd.read_csv(pd.compat.StringIO(resp.text))
                        all_rows = df.to_dict('records')
                        break
                    
                    page += 1
                    
                except requests.exceptions.SSLError as ssl_error:
                    self.logger.error(f"❌ Erro SSL (mesmo com verify=False): {ssl_error}")
                    break
                except requests.exceptions.RequestException as req_error:
                    self.logger.error(f"❌ Erro de requisição: {req_error}")
                    break
            
            # Converte para DataFrame
            if all_rows:
                df = pd.json_normalize(all_rows)
                self.logger.info(f"✅ Dados obtidos: {len(df)} registros no total")
                
                # Salva arquivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nome_arquivo = f"unipix_relatorio_{timestamp}.csv"
                caminho_arquivo = os.path.join(self.download_folder, nome_arquivo)
                
                df.to_csv(caminho_arquivo, index=False, encoding="utf-8-sig")
                self.logger.info(f"💾 Arquivo salvo: {caminho_arquivo}")
                
                return caminho_arquivo
            else:
                self.logger.warning("⚠️  Nenhum dado retornado pela API")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao baixar relatório via API: {e}")
            return None
    
    def executar_rotina_completa(self):
        """Executa toda a rotina da Unipix usando API"""
        try:
            print("\n" + "="*70)
            print("🚀 INICIANDO ROTINA UNIPIX - VERSÃO API (SSL FIX)")
            print("="*70)
            print("🔑 Usando credenciais pré-definidas...")
            print(f"👤 Usuário: {UNIPIX_USUARIO}")
            print("🔒 Senha: **********")
            print("📁 Download para: " + DOWNLOAD_FOLDER)
            print("💡 Informe apenas o período quando solicitado")
            print("🔧 SSL: Verificação desativada para resolver problema de certificado")
            
            # 1. Coletar credenciais (automático, só pede período)
            credenciais = self.coletar_credenciais_usuario()
            if not credenciais:
                return 0
            
            # 2. Fazer login e extrair token/cookies
            if not self.fazer_login_unipix(credenciais['usuario'], credenciais['senha']):
                return 0
            
            # 3. Baixar relatório via API
            arquivo_baixado = self.baixar_relatorio_via_api(credenciais['periodo'])
            
            if arquivo_baixado:
                print(f"\n🎉 ROTINA CONCLUÍDA COM SUCESSO!")
                print(f"📁 Arquivo salvo em: {arquivo_baixado}")
                return 1
            else:
                print("\n❌ Falha no download do relatório")
                return 0
                
        except Exception as e:
            self.logger.error(f"💥 Erro na rotina completa: {e}")
            print(f"💥 Erro na rotina: {e}")
            return 0
        finally:
            # Fecha o navegador
            if self.driver:
                self.driver.quit()
                self.logger.info("✅ Navegador fechado")

# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================
def main():
    try:
        config = Config()
        
        while True:
            print("\n" + "="*70)
            print("🚀 UNIPIX ETL - VERSÃO API (SSL FIX)")
            print("="*70)
            print("1 - 🏢 Web Scraping UniPix (API + Tokens + SSL Fix)")
            print("2 - 🚪 Sair")
            print("="*70)
            print("💡 Nova abordagem: Login via Selenium + Dados via API")
            print("🔧 SSL: Problema de certificado resolvido")
            
            opcao = input("\n📋 Digite sua opção (1-2): ").strip()
            
            if opcao == "1":
                scraper = UnipixScraperAPI(config, config.input_folder)
                resultado = scraper.executar_rotina_completa()
                
                if resultado > 0:
                    input(f"\n⏎ {resultado} arquivo(s) baixado(s). Enter para continuar...")
                else:
                    input("\n⏎ Nenhum arquivo baixado. Enter para continuar...")
                    
            elif opcao == "2":
                print("\n👋 Saindo do sistema...")
                break
            else:
                print("\n❌ Opção inválida!")
                input("\n⏎ Pressione Enter para continuar...")
        
    except Exception as e:
        print(f"💥 ERRO CRÍTICO: {e}")

if __name__ == "__main__":
    main()