# ==============================================================================
# Fase 2: Estrutura Luigi (Extração)
# No Luigi, o pipeline não é um único script, mas sim uma coleção de Tarefas (Tasks) que dependem umas das outras. Vamos começar com a Extração.
# ==============================================================================
import luigi
import pandas as pd
import json
import os

# Caminhos dos arquivos de output
EXTRACT_OUTPUT = 'data/faturas_extracted.csv'
TRANSFORM_OUTPUT = 'data/faturas_transformed.csv'
LOAD_OUTPUT = 'data/faturas_loaded.csv' # Arquivo final após o Load (simulação)

# Garante que o diretório 'data' exista
os.makedirs('data', exist_ok=True)

# ==============================================================================
# FASE E: EXTRAÇÃO (Luigi Task)
# - Simula a leitura do JSON e salva o CSV bruto (7 colunas)
# ==============================================================================
class ExtractTask(luigi.Task):
    """
    Extrai dados do JSON e os salva em um CSV temporário.
    """
    # 1. DEFINE O ARQUIVO DE SAÍDA (O Target)
    def output(self):
        return luigi.LocalTarget(EXTRACT_OUTPUT)

    # 2. LÓGICA DE EXECUÇÃO
    def run(self):
        print("⏳ Iniciando Tarefa: Extração de Dados (Fase E)")
        
        file_path = 'data/faturas_raw.json' # Arquivo JSON de 7 campos
        
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            # Cria o DataFrame com TODAS as 7 colunas do JSON (isso é o esperado)
            df = pd.DataFrame(data) 
            
            # Salva o DataFrame bruto no output (CSV)
            df.to_csv(self.output().path, index=False)
            
            print(f"✅ Extração Concluída. {len(df)} registros brutos salvos em: {self.output().path}")
        except FileNotFoundError:
            # Caso o JSON raw não exista
            print(f"❌ Erro: Arquivo '{file_path}' não encontrado. Crie o arquivo JSON de 7 campos.")
            # Para evitar que a TransformTask tente rodar em um arquivo que não existe
            raise 

# ==============================================================================
# FASE T: TRANSFORMAÇÃO (Luigi Task)
# - Reduz de 7 colunas para 3, limpa e anonimiza.
# ==============================================================================
class TransformTask(luigi.Task):
    """
    Transforma o CSV extraído: Projeção (7 -> 3 colunas), limpeza e anonimização.
    """
    # 1. DEFINE A DEPENDÊNCIA
    def requires(self):
        # Retorna uma lista com a tarefa que precisa ser concluída antes
        return [ExtractTask()]

    # 2. DEFINE O ARQUIVO DE SAÍDA (O Target)
    def output(self):
        return luigi.LocalTarget(TRANSFORM_OUTPUT)

    # 3. LÓGICA DE EXECUÇÃO
    def run(self):
        print("⏳ Iniciando Tarefa: Transformação de Dados (Fase T)")
        
        # *** CORREÇÃO: self.input() retorna uma lista de Targets. Pegamos o [0]. ***
        input_file = self.input()[0].path
        
        # Leitura do CSV (DataFrame com 7 Colunas)
        df = pd.read_csv(input_file)

        # *** CORREÇÃO: Projeção (7 para 3 colunas) ***
        # Usamos .iloc[:, :3] para selecionar as 3 primeiras colunas, 
        # resolvendo o problema de 'Length mismatch'
        df = df.iloc[:, :3] 

        # Renomeia as colunas (agora só tem 3)
        df.columns = ['data_emissao', 'nif_cliente', 'valor'] 
        
        # Aplicar a limpeza de valores
        # Regex=False para compatibilidade futura com Pandas
        df['valor'] = (df['valor'].astype(str)
                       .str.replace('R$', '', regex=False)
                       .str.replace(',', '.', regex=False)
                       .astype(float))

        # Governança de Dados (RGPD - Anonimização do NIF)
        df['id_anonimo'] = 'KAURA_' + (df.index + 1).astype(str)
        # Remove a coluna sensível
        df = df.drop(columns=['nif_cliente']) 
        
        # Salva o DataFrame transformado
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Transformação Concluída! Dados transformados salvos em: {self.output().path}")

# ==============================================================================
# FASE L: LOAD (Luigi Task)
# - Simula o carregamento final (poderia ser um DB, mas é um CSV)
# ==============================================================================
class LoadTask(luigi.Task):
    """
    Carrega o CSV transformado para o destino final.
    """
    # 1. DEFINE A DEPENDÊNCIA
    def requires(self):
        # Requer a TransformTask
        return [TransformTask()]

    # 2. DEFINE O ARQUIVO DE SAÍDA (O Target)
    def output(self):
        return luigi.LocalTarget(LOAD_OUTPUT)

    # 3. LÓGICA DE EXECUÇÃO
    def run(self):
        print("⏳ Iniciando Tarefa: Carregamento de Dados (Fase L)")
        
        # *** self.input() retorna uma lista de Targets. Pegamos o [0]. ***
        input_file = self.input()[0].path
        
        # Apenas simula o 'carregamento' copiando o arquivo (ou lendo e salvando)
        df = pd.read_csv(input_file)
        
        # Simula o carregamento final, renomeando o arquivo
        df.to_csv(self.output().path, index=False)
        
        print(f"✅ Carregamento Concluído! Arquivo final de destino: {self.output().path}")

# FIM DO ARQUIVO

# ==============================================================================
# PONTO DE EXECUÇÃO
# ==============================================================================
if __name__ == '__main__':
    luigi.run()