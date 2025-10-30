# main.py - ETL COM WEB SCRAPING UNIPIX + 2FA - VERSÃO ULTIMATE
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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# =============================================================================
# CONFIGURAÇÕES GLOBAIS 
# =============================================================================
UNIPIX_USUARIO = "xxxxxxxxxx"
UNIPIX_SENHA = "xxxxxxx"
DOWNLOAD_FOLDER = r"C:\Users\xxxxxxxx\Desktop\aprendizado\data\input"

# =============================================================================
# CONFIGURAÇÕES
# =============================================================================
class Config:
    def __init__(self):
        # Pastas
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.input_folder = DOWNLOAD_FOLDER  # USANDO PASTA PRÉ-DEFINIDA
        self.processed_folder = os.path.join(self.base_dir, 'data', 'processed') 
        self.error_folder = os.path.join(self.base_dir, 'data', 'error')
        self.temp_folder = os.path.join(self.base_dir, 'data', 'temp')
        
        # Criar pastas
        for folder in [self.input_folder, self.processed_folder, self.error_folder, self.temp_folder]:
            os.makedirs(folder, exist_ok=True)
        
        # Mapeamento colunas
        self.column_mapping = {
            'vendas': {
                'venda_id': 'id', 'id_venda': 'id', 'codigo': 'id',
                'data': 'data_venda', 'data_venda': 'data_venda', 'dt_venda': 'data_venda',
                'cliente': 'nome_cliente', 'nome_cliente': 'nome_cliente', 'comprador': 'nome_cliente',
                'produto': 'produto_nome', 'produto_nome': 'produto_nome', 'item': 'produto_nome',
                'quantidade': 'quantidade', 'qtd': 'quantidade', 'qnt': 'quantidade',
                'valor_unitario': 'valor_unitario', 'vl_unitario': 'valor_unitario', 'preco': 'valor_unitario',
                'valor_total': 'valor_total', 'vl_total': 'valor_total', 'total': 'valor_total',
                'regiao': 'regiao_venda', 'regional': 'regiao_venda', 'uf': 'regiao_venda'
            },
            'clientes': {
                'cliente_id': 'id', 'id_cliente': 'id', 'codigo': 'id', 'id': 'id',
                'nome': 'nome_completo', 'nome_completo': 'nome_completo', 'cliente': 'nome_completo',
                'email': 'email', 'e-mail': 'email', 'email_cliente': 'email',
                'cidade': 'cidade', 'municipio': 'cidade', 'cid': 'cidade',
                'estado': 'estado', 'uf': 'estado', 'est': 'estado',
                'data_cadastro': 'data_cadastro', 'dt_cadastro': 'data_cadastro', 'cadastro': 'data_cadastro'
            }
        }
        
        # Configurar logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger('ETL')

# =============================================================================
# LEITOR DE CÓDIGO DA PLANILHA EXCEL
# =============================================================================

class PlanilhaCodeReader:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger
        # CAMINHO CORRETO DA PLANILHA CSV
        self.caminho_planilha = r"C:\Users\marlon.carvalho\OneDrive - Ministério do Desenvolvimento e Assistência Social\Documentos\Unip\cod_unipix.csv"
    
    def aguardar_planilha_pronta(self, tempo_maximo=180):
        """Aguarda a planilha ficar disponível e estável"""
        try:
            self.logger.info(f"⏳ Aguardando planilha ficar pronta (máximo {tempo_maximo}s)...")
            
            tempo_inicio = time.time()
            ultimo_tamanho = 0
            tentativas_estavel = 0
            
            while time.time() - tempo_inicio < tempo_maximo:
                # Verifica se o arquivo existe
                if not os.path.exists(self.caminho_planilha):
                    self.logger.info("📁 Planilha ainda não encontrada, aguardando...")
                    time.sleep(5)
                    continue
                
                try:
                    # Tenta acessar o arquivo
                    tamanho_atual = os.path.getsize(self.caminho_planilha)
                    
                    # Se o tamanho mudou recentemente, espera estabilizar
                    if tamanho_atual != ultimo_tamanho:
                        self.logger.info(f"📊 Planilha detectada, tamanho: {tamanho_atual} bytes")
                        ultimo_tamanho = tamanho_atual
                        tentativas_estavel = 0
                        time.sleep(2)
                        continue
                    
                    # Se o tamanho está estável por 3 verificações, considera pronto
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
        """Lê o código da célula 1A da planilha CSV - MANTÉM FORMATAÇÃO ORIGINAL"""
        try:
            # AGUARDA A PLANILHA FICAR PRONTA PRIMEIRO
            if not self.aguardar_planilha_pronta():
                return None
            
            self.logger.info(f"📊 Lendo planilha CSV: {self.caminho_planilha}")
            
            # Tenta ler o arquivo CSV
            for tentativa in range(5):  # Tenta até 5 vezes
                try:
                    # Lê o arquivo CSV - tenta diferentes encodings
                    try:
                        df = pd.read_csv(self.caminho_planilha, header=None, encoding='utf-8')
                    except UnicodeDecodeError:
                        try:
                            df = pd.read_csv(self.caminho_planilha, header=None, encoding='latin-1')
                        except UnicodeDecodeError:
                            df = pd.read_csv(self.caminho_planilha, header=None, encoding='cp1252')
                    
                    # Verifica se tem dados
                    if df.empty:
                        self.logger.warning(f"⚠️  Planilha vazia na tentativa {tentativa + 1}")
                        time.sleep(3)
                        continue
                    
                    # A célula 1A geralmente é a linha 0, coluna 0 no pandas
                    codigo = df.iloc[0, 0] if not df.empty else None
                    
                    if pd.isna(codigo) or codigo == "" or str(codigo).strip() == "":
                        self.logger.warning(f"⚠️  Célula A1 vazia na tentativa {tentativa + 1}")
                        time.sleep(3)
                        continue
                    
                    # Converte para string e limpa - MANTÉM CASE ORIGINAL
                    codigo_str = str(codigo).strip()  # ⬅️ REMOVIDO .upper()
                    self.logger.info(f"✅ Código lido da planilha: {codigo_str}")
                    
                    # Valida o formato (agora aceita maiúsculas e minúsculas)
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
        """Valida se o código está no formato correto - AGORA CASE SENSITIVE"""
        import re
        
        # Padrões aceitáveis para código de verificação (agora case insensitive)
        padroes_validos = [
            r'^[A-Za-z0-9]{3}-[A-Za-z0-9]{3}-[A-Za-z0-9]{3}$',  # xxx-xxx-xxx (maiúsculas ou minúsculas)
            r'^[A-Za-z0-9]{6,9}$',  # 6-9 caracteres alfanuméricos
            r'^\d{6}$',  # 6 dígitos
        ]
        
        for padrao in padroes_validos:
            if re.match(padrao, codigo):
                return True
        
        self.logger.warning(f"⚠️  Formato de código não reconhecido: {codigo}")
        return False

    def debug_planilha(self):
        """Método para debug - mostra informações da planilha"""
        try:
            print("\n🔍 DEBUG DA PLANILHA")
            print("="*50)
            print(f"📁 Caminho: {self.caminho_planilha}")
            print(f"📊 Existe: {os.path.exists(self.caminho_planilha)}")
            
            if os.path.exists(self.caminho_planilha):
                print(f"📏 Tamanho: {os.path.getsize(self.caminho_planilha)} bytes")
                print(f"⏰ Modificado: {time.ctime(os.path.getmtime(self.caminho_planilha))}")
                
                # Tenta ler e mostrar conteúdo
                try:
                    with open(self.caminho_planilha, 'r', encoding='utf-8') as f:
                        conteudo = f.read()
                    print(f"📄 Conteúdo (primeiros 200 chars): {conteudo[:200]}")
                except Exception as e:
                    print(f"❌ Erro ao ler arquivo: {e}")
            else:
                print("❌ Arquivo não encontrado!")
                
        except Exception as e:
            print(f"❌ Erro no debug: {e}")

# =============================================================================
# GESTOR DE ARQUIVOS - NOVA CLASSE PARA GERENCIAR ARQUIVOS PROCESSADOS
# =============================================================================
class GestorArquivos:
    def __init__(self, config):
        self.config = config
        self.logger = config.logger
    
    def limpar_pasta_input(self):
        """Move arquivos processados para a pasta processed e limpa arquivos temporários"""
        try:
            self.logger.info("🧹 Iniciando limpeza da pasta input...")
            
            arquivos_movidos = 0
            arquivos_excluidos = 0
            
            for arquivo in os.listdir(self.config.input_folder):
                caminho_arquivo = os.path.join(self.config.input_folder, arquivo)
                
                # Move arquivos ZIP e Excel processados para a pasta processed
                if arquivo.lower().endswith(('.zip', '.xlsx', '.xls', '.csv')):
                    try:
                        destino = os.path.join(self.config.processed_folder, arquivo)
                        
                        # Se o arquivo já existe no destino, adiciona timestamp
                        if os.path.exists(destino):
                            nome, ext = os.path.splitext(arquivo)
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            novo_nome = f"{nome}_{timestamp}{ext}"
                            destino = os.path.join(self.config.processed_folder, novo_nome)
                        
                        shutil.move(caminho_arquivo, destino)
                        self.logger.info(f"✅ Arquivo movido para processed: {arquivo}")
                        arquivos_movidos += 1
                        
                    except Exception as e:
                        self.logger.error(f"❌ Erro ao mover arquivo {arquivo}: {e}")
                
                # Exclui arquivos temporários
                elif arquivo.lower().endswith(('.crdownload', '.tmp')):
                    try:
                        os.remove(caminho_arquivo)
                        self.logger.info(f"🗑️  Arquivo temporário excluído: {arquivo}")
                        arquivos_excluidos += 1
                    except Exception as e:
                        self.logger.error(f"❌ Erro ao excluir arquivo temporário {arquivo}: {e}")
            
            # Limpa pasta temp
            self.limpar_pasta_temp()
            
            self.logger.info(f"🧹 Limpeza concluída: {arquivos_movidos} arquivos movidos, {arquivos_excluidos} excluídos")
            return arquivos_movidos
            
        except Exception as e:
            self.logger.error(f"❌ Erro na limpeza da pasta input: {e}")
            return 0
    
    def limpar_pasta_temp(self):
        """Limpa completamente a pasta temporária"""
        try:
            for arquivo in os.listdir(self.config.temp_folder):
                caminho_arquivo = os.path.join(self.config.temp_folder, arquivo)
                try:
                    if os.path.isfile(caminho_arquivo):
                        os.remove(caminho_arquivo)
                    elif os.path.isdir(caminho_arquivo):
                        shutil.rmtree(caminho_arquivo)
                except Exception as e:
                    self.logger.error(f"❌ Erro ao limpar {caminho_arquivo}: {e}")
            
            self.logger.info("✅ Pasta temporária limpa")
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao limpar pasta temp: {e}")

# =============================================================================
# WEB SCRAPING ESPECÍFICO UNIPIX COM 2FA - VERSÃO ATUALIZADA COM PLANILHA
# =============================================================================
class UnipixScraper:
    def __init__(self, config, download_folder):
        self.config = config
        self.download_folder = download_folder
        self.driver = None
        self.wait = None
        self.logger = logging.getLogger('UnipixScraper')
        self.planilha_reader = PlanilhaCodeReader(config)
        self.gestor_arquivos = GestorArquivos(config)  # NOVO: Gestor de arquivos
    
    def configurar_chrome(self):
        """Configura o Chrome para download automático"""
        chrome_options = Options()
        
        # Configurações para download automático
        prefs = {
            "download.default_directory": self.download_folder,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Opções para rodar em background (remova o --headless para ver o navegador)
        # chrome_options.add_argument("--headless")  # Remova esta linha para ver o navegador
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 30)
        self.logger.info("✅ Navegador Chrome configurado")
    
    def coletar_credenciais_usuario(self):
        """Coleta credenciais e período do usuário - AGORA SÓ PERGUNTA O PERÍODO"""
        print("\n" + "="*70)
        print("🏢 UNIPIX - CONFIGURAÇÃO AUTOMÁTICA")
        print("="*70)
        
        credenciais = {}
        
        # USA CREDENCIAIS PRÉ-DEFINIDAS (automáticas)
        credenciais['usuario'] = UNIPIX_USUARIO
        credenciais['senha'] = UNIPIX_SENHA
        
        print(f"👤 Usuário: {credenciais['usuario']}")
        print("🔒 Senha: **********")
        
        # SEMPRE USA PLANILHA PARA 2FA
        credenciais['usar_planilha'] = True
        print("✅ Método 2FA: Automático (ler da planilha Excel)")
        
        # ⭐⭐ ALTERAÇÃO PRINCIPAL: SÓ PERGUNTA O PERÍODO ⭐⭐
        print("\n📅 PERÍODO DO RELATÓRIO ANALÍTICO")
        print("💡 Formato: DD/MM/AAAA - DD/MM/AAAA")
        periodo = input("📅 Digite o período (ex: 03/10/2024 - 17/10/2024): ").strip()
        
        if not periodo or ' - ' not in periodo:
            print("❌ Período no formato inválido! Use: DD/MM/AAAA - DD/MM/AAAA")
            return None
        
        credenciais['periodo'] = periodo
        
        return credenciais
    
    def obter_codigo_verificacao(self, tempo_espera=60):
        """Obtém o código de verificação da planilha - AGORA COM ESPERA"""
        try:
            self.logger.info("📊 Obtendo código da planilha...")
            print(f"⏳ Aguardando {tempo_espera} segundos para planilha atualizar...")
            
            # ESPERA O TEMPO CONFIGURADO ANTES DE TENTAR LER
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
    
    def _processar_autenticacao_2fa_automatica(self):
        """Processa autenticação 2FA usando código da planilha - VERSÃO MELHORADA"""
        try:
            print("\n📱 AUTENTICAÇÃO VIA PLANILHA EXCEL")
            print("="*50)
            print("⏳ Aguardando planilha ser atualizada (pode levar até 1 minuto)...")
            
            # DA MAIS TEMPO PARA A PLANILHA ATUALIZAR
            time.sleep(60)
            
            # Obtém código da planilha
            codigo_2fa = self.obter_codigo_verificacao()
            
            if not codigo_2fa:
                print("❌ Não foi possível obter código da planilha")
                print("🔍 Executando diagnóstico...")
                self.planilha_reader.debug_planilha()
                return self._processar_autenticacao_2fa_manual()
            
            print(f"✅ Código obtido da planilha: {codigo_2fa}")
            
            # Preenche o código no site
            return self._preencher_codigo_2fa_no_site(codigo_2fa)
            
        except Exception as e:
            self.logger.error(f"❌ Erro na autenticação automática: {e}")
            return self._processar_autenticacao_2fa_manual()
        
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
            
            return self._verificar_login_sucesso()
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao preencher código: {e}")
            return False
    
    def fazer_login_unipix(self, usuario, senha):
        """Faz login no site da Unipix com suporte a 2FA - ATUALIZADO"""
        try:
            self.logger.info("🔐 Fazendo login na Unipix...")
            print("🔐 Realizando login...")
            
            # Acessa a página de login
            self.driver.get("https://avia.unipix.com.br/#/login")
            time.sleep(5)
            
            # Aguarda a página carregar
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Tenta encontrar e preencher o campo de usuário
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
            
            # Tenta encontrar e preencher o campo de senha
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
            
            # Tenta encontrar e clicar no botão de login
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
                # AGORA USA A PLANILHA SE CONFIGURADO
                return self._processar_autenticacao_2fa_automatica()
            else:
                # Login sem 2FA
                time.sleep(5)
                return self._verificar_login_sucesso()
                
        except Exception as e:
            self.logger.error(f"❌ Erro no login: {e}")
            print(f"❌ Erro no login: {e}")
            return False
    
    def _verificar_se_precisa_2fa(self):
        """Verifica se a página pede autenticação de dois fatores"""
        try:
            # Procura por elementos indicativos de 2FA
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
    
    def _verificar_login_sucesso(self):
        """Verifica se o login foi bem-sucedido"""
        try:
            # Verifica pela URL atual
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
    
    def navegar_para_relatorios_analiticos(self):
        """Navega para a página de relatórios analíticos"""
        try:
            self.logger.info("📊 Navegando para relatórios analíticos...")
            print("📊 Acessando relatórios analíticos...")
            
            # Acessa diretamente a URL dos relatórios
            self.driver.get("https://avia.unipix.com.br/#/relatorios/analitico")
            time.sleep(8)
            
            # Aguarda a página carregar completamente
            self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            self.logger.info("✅ Página de relatórios analíticos carregada")
            print("✅ Página de relatórios carregada")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao navegar para relatórios: {e}")
            print(f"❌ Erro ao acessar relatórios: {e}")
            return False

    def aplicar_filtros_avancados(self, periodo):
        try:
            self.logger.info("🎯 Iniciando aplicação de filtros avançados...")
            print("🎯 Aplicando filtros avançados...")
            
            # AGUARDA UM POUCO MAIS PARA A PÁGINA CARREGAR COMPLETAMENTE
            time.sleep(5)
            
            # 1. CLICAR EM "FILTROS AVANÇADOS" - OTIMIZADO: SELEtor 6 PRIMEIRO
            seletores_filtro_avancado = [
                # PRIMEIRO: Seletor que sabemos que funciona (número 6 original)
                "//button[.//img[contains(@src, 'filter.svg')]]",
                
                # DEPOIS: Outros seletores como fallback
                "//button[.//*[contains(@src, 'filter')]]",
                "//button[.//span[contains(text(), 'Filtros avançados')]]",
                "//span[contains(text(), 'Filtros avançados')]",
                "//*[contains(text(), 'Filtros avançados')]",
                "//span[@class='mat-button-wrapper']//span[contains(text(), 'Filtros avançados')]",
                "//button[contains(@class, 'mat-button')]//span[contains(text(), 'Filtros avançados')]",
                "//button[contains(., 'Filtros')]",
                "//span[contains(., 'Filtros')]//parent::button",
                "//button[contains(@class, 'btn') and contains(., 'Filtro')]",
                "//button[contains(@class, 'filter')]",
            ]
            
            botao_filtros = None
            seletor_usado = None
            
            for i, seletor in enumerate(seletores_filtro_avancado, 1):
                try:
                    # LOG DIFERENCIADO PARA O SELETOR PREFERENCIAL
                    if i == 1:
                        self.logger.info(f"🎯 Tentando seletor PREFERENCIAL: {seletor}")
                        print("🎯 Usando seletor preferencial...")
                    else:
                        self.logger.info(f"🔍 Tentando seletor fallback {i}: {seletor}")
                    
                    botao_filtros = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, seletor))
                    )
                    
                    # Rolando para o elemento ser visível
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", botao_filtros)
                    time.sleep(1)
                    
                    # Tenta clicar via JavaScript se o clique normal falhar
                    try:
                        botao_filtros.click()
                    except:
                        self.driver.execute_script("arguments[0].click();", botao_filtros)
                    
                    seletor_usado = seletor
                    if i == 1:
                        self.logger.info("✅ Seletor PREFERENCIAL funcionou!")
                        print("✅ Filtros avançados abertos com sucesso!")
                    else:
                        self.logger.info(f"✅ Seletor fallback {i} funcionou: {seletor}")
                    break
                    
                except Exception as e:
                    if i == 1:
                        self.logger.warning(f"⚠️  Seletor preferencial falhou, tentando fallbacks...")
                    else:
                        self.logger.info(f"❌ Seletor {i} falhou: {e}")
                    continue
            
            if not botao_filtros:
                self.logger.error("❌ Botão 'Filtros avançados' não encontrado")
                print("🔍 Tentando encontrar botões manualmente...")
                
                # DEBUG SIMPLIFICADO
                try:
                    botoes = self.driver.find_elements(By.TAG_NAME, "button")
                    botoes_com_texto = [b for b in botoes if b.text.strip()]
                    print(f"📋 Botões encontrados: {len(botoes_com_texto)}")
                    
                    for btn in botoes_com_texto[:5]:  # Mostra apenas os 5 primeiros
                        print(f"   📝 '{btn.text}'")
                except:
                    pass
                    
                return False
            
            time.sleep(3)
            
            # 2. PROCURA PELO INPUT "DATA DE ENVIO" - TAMBÉM OTIMIZADO
            seletores_data_envio = [
                "//input[@placeholder='Data de envio']",
                "//input[contains(@placeholder, 'Data de envio')]",
                "//input[@ngxdaterangepickermd]",
                "//input[contains(@class, 'ng-pristine') and contains(@placeholder, 'Data')]",
            ]
            
            input_data = None
            for seletor in seletores_data_envio:
                try:
                    input_data = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, seletor))
                    )
                    input_data.click()
                    self.logger.info("✅ Input 'Data de envio' clicado")
                    break
                except:
                    continue
            
            if not input_data:
                self.logger.error("❌ Input 'Data de envio' não encontrado")
                return False
            
            time.sleep(2)
            
            # 3. PREENCHER DATA 
            self.logger.info(f"📅 Preenchendo período: {periodo}")

            # Abordagem 1: Tentar preencher parte por parte com delays
            try:
                # Primeiro limpa o campo completamente
                input_data.clear()
                time.sleep(1)
                
                # Preenche caractere por caractere simulando digitação humana
                for char in periodo:
                    input_data.send_keys(char)
                    time.sleep(0.1)  # Pequeno delay entre cada caractere
                
                self.logger.info("✅ Data preenchida via send_keys lento")
                
            except Exception as keys_error:
                self.logger.warning(f"⚠️  Send_keys lento falhou: {keys_error}")
                
                # Abordagem 2: JavaScript com mais eventos
                try:
                    script = f"""
                    var input = arguments[0];
                    
                    // Limpa o campo
                    input.value = '';
                    
                    // Dispara eventos de limpeza
                    input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                    input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                    input.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                    
                    // Pequeno delay antes de preencher
                    setTimeout(function() {{
                        input.value = '{periodo}';
                        
                        // Dispara todos os eventos possíveis
                        input.dispatchEvent(new Event('input', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('change', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('keydown', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('keyup', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('keypress', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('focus', {{ bubbles: true }}));
                        input.dispatchEvent(new Event('blur', {{ bubbles: true }}));
                        
                        // Eventos específicos do Angular
                        input.dispatchEvent(new Event('ngModelChange', {{ bubbles: true }}));
                    }}, 100);
                    """
                    time.sleep(10)
                    
                    self.driver.execute_script(script, input_data)
                    self.logger.info("✅ Data preenchida via JavaScript avançado")
                    
                except Exception as js_error:
                    self.logger.warning(f"⚠️  JavaScript avançado falhou: {js_error}")
                    
                    # Abordagem 3: Clique fora do campo para forçar o blur
                    try:
                        input_data.clear()
                        time.sleep(0.5)
                        input_data.send_keys(periodo)
                        time.sleep(0.5)
                        
                        # Clica em outro elemento para forçar o blur
                        body = self.driver.find_element(By.TAG_NAME, "body")
                        body.click()
                        self.logger.info("✅ Data preenchida com blur forçado")
                        
                    except Exception as final_error:
                        self.logger.error(f"❌ Todas as abordagens falharam: {final_error}")
                        return False

            time.sleep(2)

            # VERIFICAÇÃO EXTRA: Confirmar que a data ficou correta
            try:
                valor_atual = input_data.get_attribute('value')
                if valor_atual != periodo:
                    self.logger.warning(f"⚠️  Data foi alterada: Esperado '{periodo}', obtido '{valor_atual}'")
                    
                    # Tenta corrigir se foi alterada
                    input_data.clear()
                    time.sleep(0.5)
                    input_data.send_keys(periodo)
                    time.sleep(0.5)
                    
                    # Clica fora novamente
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    body.click()
                    time.sleep(1)
                    
                    # Verifica novamente
                    valor_corrigido = input_data.get_attribute('value')
                    if valor_corrigido != periodo:
                        self.logger.error(f"❌ Não foi possível corrigir a data: {valor_corrigido}")
                        return False
                    else:
                        self.logger.info("✅ Data corrigida com sucesso")
                        
            except Exception as verify_error:
                self.logger.warning(f"⚠️  Não foi possível verificar a data: {verify_error}")

            time.sleep(2)

            # 4. CONFIRMAR DATA - OTIMIZADO
            seletores_confirmar = [
                "//button[contains(text(), 'Confirmar')]",
                "//button[text()='Confirmar']",
            ]

            for seletor in seletores_confirmar:
                try:
                    botao_confirmar = self.driver.find_element(By.XPATH, seletor)
                    if botao_confirmar.is_displayed():
                        self.driver.execute_script("arguments[0].click();", botao_confirmar)
                        self.logger.info("✅ Data confirmada")
                        break
                except:
                    continue

            time.sleep(2)

            
            # 5. APLICAR FILTROS - OTIMIZADO
            seletores_aplicar_filtros = [
                "//button[.//img[contains(@src, 'filterReport.svg')]]",
                "//button[contains(text(), 'Aplicar Filtros')]",
            ]
            
            botao_aplicar = None
            for seletor in seletores_aplicar_filtros:
                try:
                    botao_aplicar = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, seletor))
                    )
                    self.driver.execute_script("arguments[0].click();", botao_aplicar)
                    self.logger.info("✅ Filtros aplicados")
                    break
                except:
                    continue
            
            if not botao_aplicar:
                self.logger.error("❌ Botão 'Aplicar Filtros' não encontrado")
                return False
            
            self.logger.info("⏳ Aguardando processamento...")
            print("⏳ Processando dados...")
            
            time.sleep(10)
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao aplicar filtros: {e}")
            return False

    def baixar_planilha_csv(self):
        """Baixa planilha no formato CSV"""
        try:
            self.logger.info("📥 Iniciando download da planilha CSV...")
            print("📥 Iniciando download como CSV...")
            
            # 1. CLICAR EM "BAIXAR PLANILHA"
            seletores_baixar = [
                "//span[contains(@class, 'mat-button-wrapper')]//img[contains(@src, 'fi_save.svg')]//ancestor::span//ancestor::button",
                "//button[.//img[contains(@src, 'fi_save.svg')]]",
                "//span[contains(text(), 'Baixar planilha')]//ancestor::button",
                "//button[.//span[contains(text(), 'Baixar planilha')]]"
            ]
            
            botao_baixar = None
            for seletor in seletores_baixar:
                try:
                    botao_baixar = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, seletor))
                    )
                    botao_baixar.click()
                    self.logger.info("✅ Botão 'Baixar planilha' clicado")
                    break
                except:
                    continue
            
            if not botao_baixar:
                self.logger.error("❌ Botão 'Baixar planilha' não encontrado")
                return None
            
            time.sleep(3)
            
            # 2. SELECIONAR FORMATO CSV
            seletores_csv = [
                "//button[contains(@class, 'button-') and contains(text(), 'CSV')]",
                "//button[text()='CSV']",
                "//button[contains(text(), 'CSV') and contains(@class, 'ng-star-inserted')]"
            ]
            
            botao_csv = None
            for seletor in seletores_csv:
                try:
                    botao_csv = self.wait.until(
                        EC.element_to_be_clickable((By.XPATH, seletor))
                    )
                    botao_csv.click()
                    self.logger.info("✅ Opção 'CSV' selecionada")
                    break
                except:
                    continue
            
            if not botao_csv:
                self.logger.error("❌ Botão 'CSV' não encontrado")
                return None
            
            # 3. AGUARDAR DOWNLOAD - VERSÃO SIMPLIFICADA SEM LOOP
            self.logger.info("⏳ Aguardando conclusão do download...")
            print("⏳ Aguardando conclusão do download (100 segundos)...")
            
            # Espera fixa suficiente para o download - SEM LOOP COMPLEXO
            time.sleep(100)
            
            # Procura por qualquer arquivo ZIP/CSV na pasta de download
            arquivos_download = []
            for arquivo in os.listdir(self.download_folder):
                if arquivo.lower().endswith(('.zip', '.csv', '.xlsx', '.xls')):
                    caminho_completo = os.path.join(self.download_folder, arquivo)
                    arquivos_download.append(caminho_completo)
            
            if arquivos_download:
                # Pega o primeiro arquivo encontrado (ou poderia pegar o mais recente)
                arquivo_baixado = arquivos_download[0]
                nome_arquivo = os.path.basename(arquivo_baixado)
                
                self.logger.info(f"✅ Arquivo encontrado: {nome_arquivo}")
                print(f"✅ Download detectado: {nome_arquivo}")
                return arquivo_baixado
            else:
                # Se não encontrou arquivos, apenas continua o processo
                self.logger.info("⚠️  Nenhum arquivo de download encontrado, mas continuando processo")
                print("⚠️  Nenhum arquivo detectado, mas continuando processo...")
                return None
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao baixar planilha CSV: {e}")
            print(f"❌ Erro no download: {e}")
            return None
        
    def executar_rotina_completa(self):

        """Executa toda a rotina da Unipix - VERSÃO ULTIMATE"""
        try:
            print("\n" + "="*70)
            print("🚀 INICIANDO ROTINA UNIPIX - VERSÃO ULTIMATE")
            print("="*70)
            print("🔑 Usando credenciais pré-definidas...")
            print(f"👤 Usuário: {UNIPIX_USUARIO}")
            print("🔒 Senha: **********")
            print("📁 Download para: " + DOWNLOAD_FOLDER)
            print("💡 Informe apenas o período quando solicitado")
            
            # 1. Coletar credenciais (agora automático, só pede período)
            credenciais = self.coletar_credenciais_usuario()
            if not credenciais:
                return 0
        
        
            
            # 2. Configurar navegador
            self.configurar_chrome()
            
            # 3. Fazer login (agora com suporte a 2FA via planilha)
            if not self.fazer_login_unipix(credenciais['usuario'], credenciais['senha']):
                return 0
            
            # 4. Navegar para relatórios
            if not self.navegar_para_relatorios_analiticos():
                return 0
            
            # 5. Aplicar filtros avançados
            if not self.aplicar_filtros_avancados(credenciais['periodo']):
                return 0
            
            # 6. Baixar planilha como CSV
            arquivo_baixado = self.baixar_planilha_csv()
            
            if arquivo_baixado:
                print(f"\n🎉 ROTINA CONCLUÍDA COM SUCESSO!")
                print(f"📁 Arquivo salvo em: {arquivo_baixado}")
                
                return 1
            else:
                print("\n❌ Falha no download do arquivo")
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
# BANCO DE DADOS SIMULADO 
# =============================================================================
class DatabaseSimulado:
    def __init__(self, logger):
        self.logger = logger
        self.conn = sqlite3.connect(':memory:', check_same_thread=False)
        self.estado_inicial = None
        self.criar_tabelas()
        self.salvar_estado_inicial()
    
    def salvar_estado_inicial(self):
        """Salva o estado inicial do banco (vazio)"""
        self.estado_inicial = {
            'vendas': self.consultar_dados("SELECT * FROM vendas"),
            'clientes': self.consultar_dados("SELECT * FROM clientes")
        }
    
    def criar_tabelas(self):
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vendas (
                id INTEGER PRIMARY KEY,
                data_venda TEXT,
                nome_cliente TEXT,
                produto_nome TEXT,
                quantidade INTEGER,
                valor_unitario REAL,
                valor_total REAL,
                regiao_venda TEXT,
                data_processamento TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS clientes (
                id INTEGER PRIMARY KEY,
                nome_completo TEXT,
                email TEXT,
                cidade TEXT,
                estado TEXT,
                data_cadastro TEXT,
                data_processamento TEXT
            )
        ''')
        
        self.conn.commit()
        self.logger.info("✅ Tabelas do banco criadas")
    
    def mostrar_estrutura_completa(self):
        """Mostra estrutura completa do banco"""
        print("\n" + "="*70)
        print("🏗️  ESTRUTURA COMPLETA DO BANCO DE DADOS")
        print("="*70)
        
        # Mostrar todas as tabelas
        tabelas = self.consultar_dados("SELECT name FROM sqlite_master WHERE type='table'")
        print(f"\n📋 TABELAS EXISTENTES ({len(tabelas)}):")
        for _, tabela in tabelas.iterrows():
            print(f"   • {tabela['name']}")
        
        # Mostrar estrutura de cada tabela
        for _, tabela in tabelas.iterrows():
            nome_tabela = tabela['name']
            print(f"\n📊 TABELA: {nome_tabela}")
            
            # Estrutura das colunas
            colunas = self.consultar_dados(f"PRAGMA table_info({nome_tabela})")
            print("   COLUNAS:")
            for _, coluna in colunas.iterrows():
                pk = " 🔑" if coluna['pk'] == 1 else ""
                print(f"     - {coluna['name']} ({coluna['type']}){pk}")
            
            # Quantidade de registros
            count = self.consultar_dados(f"SELECT COUNT(*) as total FROM {nome_tabela}")
            print(f"   📈 TOTAL DE REGISTROS: {count['total'].iloc[0]}")
    
    def mostrar_dados_tabelas(self, estado="atual"):
        """Mostra todos os dados das tabelas"""
        if estado == "inicial":
            dados_vendas = self.estado_inicial['vendas']
            dados_clientes = self.estado_inicial['clientes']
            titulo = "ESTADO INICIAL DO BANCO (ANTES DO ETL)"
        else:
            dados_vendas = self.consultar_dados("SELECT * FROM vendas")
            dados_clientes = self.consultar_dados("SELECT * FROM clientes")
            titulo = "ESTADO ATUAL DO BANCO (APÓS O ETL)"
        
        print(f"\n" + "="*70)
        print(f"📊 {titulo}")
        print("="*70)
        
        # Tabela VENDAS
        print(f"\n🛒 TABELA VENDAS:")
        if dados_vendas.empty:
            print("   (Nenhum registro)")
        else:
            print(dados_vendas.to_string(index=False))
            print(f"   📊 Total: {len(dados_vendas)} registros")
        
        # Tabela CLIENTES
        print(f"\n👥 TABELA CLIENTES:")
        if dados_clientes.empty:
            print("   (Nenhum registro)")
        else:
            print(dados_clientes.to_string(index=False))
            print(f"   📊 Total: {len(dados_clientes)} registros")
    
    def mostrar_comparacao(self):
        """Mostra comparação entre estado inicial e atual"""
        print("\n" + "="*70)
        print("📈 COMPARAÇÃO: ANTES vs DEPOIS DO ETL")
        print("="*70)
        
        # Vendas
        vendas_inicial = len(self.estado_inicial['vendas'])
        vendas_atual = len(self.consultar_dados("SELECT * FROM vendas"))
        print(f"\n🛒 VENDAS:")
        print(f"   • Antes: {vendas_inicial} registros")
        print(f"   • Depois: {vendas_atual} registros")
        print(f"   • Diferença: +{vendas_atual - vendas_inicial} registros")
        
        # Clientes
        clientes_inicial = len(self.estado_inicial['clientes'])
        clientes_atual = len(self.consultar_dados("SELECT * FROM clientes"))
        print(f"\n👥 CLIENTES:")
        print(f"   • Antes: {clientes_inicial} registros")
        print(f"   • Depois: {clientes_atual} registros")
        print(f"   • Diferença: +{clientes_atual - clientes_inicial} registros")
    
    def converter_tipos_para_sqlite(self, df):
        """Converte tipos do pandas para compatíveis com SQLite"""
        df_converted = df.copy()
        
        for coluna in df_converted.columns:
            if pd.api.types.is_datetime64_any_dtype(df_converted[coluna]):
                df_converted[coluna] = df_converted[coluna].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif pd.api.types.is_numeric_dtype(df_converted[coluna]):
                if df_converted[coluna].dtype == np.int64:
                    df_converted[coluna] = df_converted[coluna].astype(int)
                else:
                    df_converted[coluna] = df_converted[coluna].astype(float)
            else:
                df_converted[coluna] = df_converted[coluna].astype(str)
        
        return df_converted
    
    def inserir_dados(self, tabela, dados):
        try:
            if dados.empty:
                return False
            
            dados_convertidos = self.converter_tipos_para_sqlite(dados)
            
            records = [tuple(x) for x in dados_convertidos.to_numpy()]
            columns = ', '.join(dados_convertidos.columns)
            placeholders = ', '.join(['?' for _ in dados_convertidos.columns])
            
            query = f"INSERT INTO {tabela} ({columns}) VALUES ({placeholders})"
            
            cursor = self.conn.cursor()
            cursor.executemany(query, records)
            self.conn.commit()
            
            self.logger.info(f"✅ {len(records)} registros inseridos em {tabela}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao inserir em {tabela}: {e}")
            return False
    
    def consultar_dados(self, query):
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            self.logger.error(f"❌ Erro na consulta: {e}")
            return pd.DataFrame()

# =============================================================================
# PROCESSADOR ETL - ATUALIZADO COM GESTOR DE ARQUIVOS
# =============================================================================
class ProcessadorETL:
    def __init__(self, config, database):
        self.config = config
        self.database = database
        self.logger = config.logger
        self.gestor_arquivos = GestorArquivos(config)  # NOVO: Gestor de arquivos
    
    def listar_arquivos_entrada(self):
        """Lista todos os arquivos de entrada (ZIP e Excel)"""
        arquivos = []
        
        for arquivo in os.listdir(self.config.input_folder):
            caminho_completo = os.path.join(self.config.input_folder, arquivo)
            
            # Aceita ZIP e Excel
            if arquivo.lower().endswith(('.xlsx', '.xls', '.zip', '.csv')):
                arquivos.append({
                    'caminho': caminho_completo,
                    'nome': arquivo,
                    'tipo': 'zip' if arquivo.lower().endswith('.zip') else 'excel'
                })
        
        self.logger.info(f"📂 Encontrados {len(arquivos)} arquivos de entrada")
        return arquivos
    
    def extrair_arquivo_zip(self, caminho_zip):
        """Extrai arquivos ZIP para pasta temporária"""
        try:
            pasta_destino = self.config.temp_folder
            
            with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            
            # Lista arquivos extraídos
            arquivos_extraidos = []
            for arquivo in os.listdir(pasta_destino):
                if arquivo.lower().endswith(('.xlsx', '.xls', '.csv')):
                    arquivos_extraidos.append(os.path.join(pasta_destino, arquivo))
            
            self.logger.info(f"✅ ZIP extraído: {len(arquivos_extraidos)} arquivos")
            return arquivos_extraidos
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao extrair ZIP {caminho_zip}: {e}")
            return []
    
    def detectar_tipo_dados(self, df):
        """Detecta automaticamente se são dados de vendas ou clientes"""
        try:
            colunas = [col.lower() for col in df.columns]
            
            # Palavras-chave para identificar tipo de dados
            indicadores_vendas = ['venda', 'produto', 'quantidade', 'valor', 'preço', 'total']
            indicadores_clientes = ['cliente', 'email', 'cidade', 'estado', 'cadastro', 'endereço']
            
            score_vendas = sum(1 for indicador in indicadores_vendas 
                             if any(indicador in coluna for coluna in colunas))
            score_clientes = sum(1 for indicador in indicadores_clientes 
                               if any(indicador in coluna for coluna in colunas))
            
            if score_vendas > score_clientes:
                return 'vendas'
            elif score_clientes > score_vendas:
                return 'clientes'
            else:
                # Se empate, verifica pelas colunas específicas
                if any('venda' in col for col in colunas) or any('produto' in col for col in colunas):
                    return 'vendas'
                elif any('cliente' in col for col in colunas) or any('email' in col for col in colunas):
                    return 'clientes'
                else:
                    return 'vendas'  # Default para vendas
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao detectar tipo de dados: {e}")
            return 'vendas'
    
    def normalizar_colunas(self, df, tipo_dados):
        """Normaliza nomes das colunas conforme mapeamento"""
        try:
            df_normalizado = df.copy()
            df_normalizado.columns = [col.strip().lower() for col in df_normalizado.columns]
            
            mapeamento = self.config.column_mapping.get(tipo_dados, {})
            colunas_mapeadas = {}
            
            for coluna_original in df_normalizado.columns:
                coluna_normalizada = None
                
                # Procura por correspondência exata ou parcial
                for chave, valor in mapeamento.items():
                    if chave in coluna_original:
                        coluna_normalizada = valor
                        break
                
                if coluna_normalizada:
                    colunas_mapeadas[coluna_original] = coluna_normalizada
                else:
                    # Mantém a coluna original se não encontrar mapeamento
                    colunas_mapeadas[coluna_original] = coluna_original
            
            df_normalizado = df_normalizado.rename(columns=colunas_mapeadas)
            
            # Adiciona data de processamento
            df_normalizado['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            self.logger.info(f"✅ Colunas normalizadas para {tipo_dados}")
            return df_normalizado
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao normalizar colunas: {e}")
            return df
    
    def processar_arquivo_excel(self, caminho_arquivo):
        """Processa arquivo Excel individual"""
        try:
            self.logger.info(f"📊 Processando arquivo: {os.path.basename(caminho_arquivo)}")
            
            # Lê o arquivo Excel
            df = pd.read_excel(caminho_arquivo)
            self.logger.info(f"✅ Arquivo lido: {len(df)} linhas, {len(df.columns)} colunas")
            
            # Detecta tipo de dados
            tipo_dados = self.detectar_tipo_dados(df)
            self.logger.info(f"🔍 Tipo detectado: {tipo_dados}")
            
            # Normaliza colunas
            df_normalizado = self.normalizar_colunas(df, tipo_dados)
            
            # Insere no banco
            if self.database.inserir_dados(tipo_dados, df_normalizado):
                self.logger.info(f"💾 Dados inseridos na tabela {tipo_dados}")
                return True
            else:
                self.logger.error(f"❌ Falha ao inserir na tabela {tipo_dados}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo Excel {caminho_arquivo}: {e}")
            return False
    
    def processar_arquivo_csv(self, caminho_arquivo):
        """Processa arquivo CSV individual"""
        try:
            self.logger.info(f"📊 Processando arquivo CSV: {os.path.basename(caminho_arquivo)}")
            
            # Lê o arquivo CSV
            df = pd.read_csv(caminho_arquivo)
            self.logger.info(f"✅ Arquivo CSV lido: {len(df)} linhas, {len(df.columns)} colunas")
            
            # Detecta tipo de dados
            tipo_dados = self.detectar_tipo_dados(df)
            self.logger.info(f"🔍 Tipo detectado: {tipo_dados}")
            
            # Normaliza colunas
            df_normalizado = self.normalizar_colunas(df, tipo_dados)
            
            # Insere no banco
            if self.database.inserir_dados(tipo_dados, df_normalizado):
                self.logger.info(f"💾 Dados inseridos na tabela {tipo_dados}")
                return True
            else:
                self.logger.error(f"❌ Falha ao inserir na tabela {tipo_dados}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo CSV {caminho_arquivo}: {e}")
            return False
    
    def processar_arquivo(self, info_arquivo):
        """Processa um arquivo (ZIP, Excel ou CSV)"""
        try:
            caminho = info_arquivo['caminho']
            tipo = info_arquivo['tipo']
            
            if tipo == 'zip':
                # Extrai e processa arquivos do ZIP
                arquivos_extraidos = self.extrair_arquivo_zip(caminho)
                if not arquivos_extraidos:
                    return False
                
                sucesso_total = True
                for arquivo in arquivos_extraidos:
                    if arquivo.lower().endswith('.csv'):
                        sucesso = self.processar_arquivo_csv(arquivo)
                    else:
                        sucesso = self.processar_arquivo_excel(arquivo)
                    sucesso_total = sucesso_total and sucesso
                
                return sucesso_total
                
            else:
                # Processa arquivo direto
                if caminho.lower().endswith('.csv'):
                    return self.processar_arquivo_csv(caminho)
                else:
                    return self.processar_arquivo_excel(caminho)
                    
        except Exception as e:
            self.logger.error(f"❌ Erro ao processar arquivo {info_arquivo['nome']}: {e}")
            return False
    
    def executar_etl(self):
        """Executa o processo ETL completo com limpeza automática"""
        self.logger.info("=" * 60)
        self.logger.info("🚀 INICIANDO PROCESSO ETL")
        self.logger.info("=" * 60)
        
        arquivos = self.listar_arquivos_entrada()
        
        if not arquivos:
            self.logger.info("📭 Nenhum arquivo encontrado na pasta 'data/input'")
            self.logger.info("💡 Coloque arquivos Excel ou ZIP na pasta 'data/input' e execute novamente")
            return 0
        
        sucessos = 0
        for info_arquivo in arquivos:
            if self.processar_arquivo(info_arquivo):
                sucessos += 1
        
        # NOVO: Limpeza automática após processamento
        if sucessos > 0:
            self.logger.info("🧹 Executando limpeza automática pós-processamento...")
            arquivos_movidos = self.gestor_arquivos.limpar_pasta_input()
            self.logger.info(f"✅ {arquivos_movidos} arquivos movidos para pasta 'processed'")
        
        self.logger.info("=" * 60)
        self.logger.info(f"📊 PROCESSAMENTO CONCLUÍDO!")
        self.logger.info(f"✅ {sucessos} arquivos processados com sucesso")
        self.logger.info(f"❌ {len(arquivos) - sucessos} arquivos com erro")
        
        return sucessos

# =============================================================================
# SISTEMA INTERATIVO DE VISUALIZAÇÃO (MANTIDO IGUAL)
# =============================================================================
def mostrar_menu_visualizacao(database):
    """Menu interativo para visualizar o banco"""
    while True:
        print("\n" + "="*70)
        print("🔍 SISTEMA DE VISUALIZAÇÃO DO BANCO DE DADOS")
        print("="*70)
        print("1 - Visualizar estado INICIAL do banco (antes do ETL)")
        print("2 - Visualizar estado ATUAL do banco (após o ETL)")
        print("3 - Visualizar COMPARAÇÃO entre antes e depois")
        print("4 - Visualizar ESTRUTURA completa do banco")
        print("5 - Sair do sistema de visualização")
        print("="*70)
        
        opcao = input("\n📋 Digite sua opção (1-5): ").strip()
        
        if opcao == "1":
            database.mostrar_dados_tabelas(estado="inicial")
        elif opcao == "2":
            database.mostrar_dados_tabelas(estado="atual")
        elif opcao == "3":
            database.mostrar_comparacao()
        elif opcao == "4":
            database.mostrar_estrutura_completa()
        elif opcao == "5":
            print("\n👋 Saindo do sistema de visualização...")
            break
        else:
            print("\n❌ Opção inválida! Digite um número entre 1 e 5.")
        
        input("\n⏎ Pressione Enter para continuar...")

# =============================================================================
# FUNÇÃO PRINCIPAL DO WEB SCRAPING UNIPIX
# =============================================================================

def executar_web_scraping_unipix(config):
    """Executa web scraping específico para UniPix"""
    try:
        scraper = UnipixScraper(config, config.input_folder)
        resultado = scraper.executar_rotina_completa()
        return resultado
        
    except Exception as e:
        print(f"💥 Erro no web scraping UniPix: {e}")
        return 0

def main():
    try:
        config = Config()
        database = DatabaseSimulado(config.logger)
        etl = ProcessadorETL(config, database)
        
        while True:
            print("\n" + "="*70)
            print("🚀 SISTEMA ETL COMPLETO - UNIPIX ULTIMATE")
            print("="*70)
            print("1 - 🏢 Web Scraping UniPix (AUTOMÁTICO - Credenciais pré-definidas)")
            print("2 - ⚙️  Executar ETL (processar arquivos + limpeza automática)")
            print("3 - 📊 Visualizar Banco de Dados")
            print("4 - 🚪 Sair")
            print("="*70)
            print("💡 Dica: Web Scraping usa credenciais automáticas e faz limpeza!")
            
            opcao = input("\n📋 Digite sua opção (1-4): ").strip()
            
            if opcao == "1":
                arquivos_baixados = executar_web_scraping_unipix(config)
                if arquivos_baixados > 0:
                    input(f"\n⏎ {arquivos_baixados} arquivos baixados. Enter para continuar...")
                else:
                    input("\n⏎ Nenhum arquivo baixado. Enter para continuar...")
                    
            elif opcao == "2":
                print("\n" + "="*70)
                print("🎯 ESTADO INICIAL DO BANCO")
                print("="*70)
                database.mostrar_estrutura_completa()
                database.mostrar_dados_tabelas(estado="inicial")
                
                input("\n⏎ Pressione Enter para iniciar o processamento ETL...")
                
                sucessos = etl.executar_etl()
                
                if sucessos > 0:
                    print("\n" + "="*70)
                    print("🎉 PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
                    print("="*70)
                    mostrar_menu_visualizacao(database)
                else:
                    print("\n⚠️  Nenhum arquivo foi processado com sucesso.")
                    input("\n⏎ Pressione Enter para continuar...")
                    
            elif opcao == "3":
                mostrar_menu_visualizacao(database)
                
            elif opcao == "4":
                print("\n👋 Saindo do sistema...")
                break
            else:
                print("\n❌ Opção inválida!")
                input("\n⏎ Pressione Enter para continuar...")
        
    except Exception as e:
        print(f"💥 ERRO CRÍTICO: {e}")

if __name__ == "__main__":

    main()
