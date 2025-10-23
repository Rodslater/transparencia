library(dplyr)
library(readr)
library(tidyr)
library(lubridate)
library(stringr)
library(jsonlite)
library(httr)

# Configurações
url_zip <- "https://dadosabertos-download.cgu.gov.br/dados_e-agendas/dados_e-agendas.zip"
dir_temp <- tempdir()
arquivo_zip <- file.path(dir_temp, "dados_e-agendas.zip")
dir_extracao <- file.path(dir_temp, "extracted_csvs")
entidade_alvo <- "Instituto Federal de Educação, Ciência e Tecnologia de Sergipe"

# 1. Download do arquivo ZIP
cat("Baixando arquivo ZIP...\n")
download.file(url_zip, arquivo_zip, mode = "wb", quiet = FALSE)
cat("Download concluído!\n\n")

# 2. Extrair arquivos
cat("Extraindo arquivos...\n")
if (dir.exists(dir_extracao)) {
  unlink(dir_extracao, recursive = TRUE)
}
dir.create(dir_extracao, recursive = TRUE)
unzip(arquivo_zip, exdir = dir_extracao)
cat("Extração concluída!\n\n")

# 3. Listar arquivos CSV
arquivos_csv <- list.files(dir_extracao, pattern = "\\.csv$", 
                           full.names = TRUE, recursive = TRUE)
cat(sprintf("Encontrados %d arquivos CSV\n\n", length(arquivos_csv)))

# 4. Processar e filtrar CSVs
cat("Processando arquivos CSV...\n")
lista_dfs <- list()

for (csv_path in arquivos_csv) {
  arquivo_nome <- basename(csv_path)
  tamanho <- file.size(csv_path)
  cat(sprintf("\nProcessando: %s (%.2f MB)\n", arquivo_nome, tamanho / 1024^2))
  
  tryCatch({
    # Ler CSV com encoding UTF-8
    df <- read_csv(csv_path, 
                   locale = locale(encoding = "UTF-8"),
                   col_types = cols(.default = "c"),
                   show_col_types = FALSE)
    
    # Verificar se coluna existe
    if ("Órgão/Entidade" %in% colnames(df)) {
      # Filtrar pela entidade
      df_filtrado <- df %>%
        mutate(`Órgão/Entidade` = str_trim(`Órgão/Entidade`)) %>%
        filter(`Órgão/Entidade` == entidade_alvo)
      
      if (nrow(df_filtrado) > 0) {
        cat(sprintf("  -> %d registros encontrados\n", nrow(df_filtrado)))
        lista_dfs[[length(lista_dfs) + 1]] <- df_filtrado
      } else {
        cat("  -> Nenhum registro encontrado\n")
      }
    } else {
      cat("  -> Aviso: Coluna 'Órgão/Entidade' não encontrada\n")
    }
    
  }, error = function(e) {
    cat(sprintf("  -> Erro ao processar: %s\n", e$message))
  })
}

# 5. Consolidar resultados
if (length(lista_dfs) > 0) {
  cat("\n--- RESULTADO CONSOLIDADO ---\n")
  resultado_final <- bind_rows(lista_dfs)
  cat(sprintf("Total de %d registros encontrados\n\n", nrow(resultado_final)))
  
  # 6. Tratamento para Supabase
  cat("--- TRATAMENTO DOS DADOS ---\n")
  
  resultado_final <- resultado_final %>%
    # Renomear entidade para IFS
    mutate(`Órgão/Entidade` = if_else(
      `Órgão/Entidade` == entidade_alvo,
      "IFS",
      `Órgão/Entidade`
    )) %>%
    # Substituir "ND" e strings vazias por NA
    mutate(across(everything(), ~na_if(., "ND"))) %>%
    mutate(across(everything(), ~na_if(., ""))) %>%
    # Renomear colunas para snake_case
    rename(
      nome = `Nome`,
      cargo_funcao = `Cargo/Função`,
      tipo_exercicio = `Tipo de exercício`,
      orgao_entidade = `Órgão/Entidade`,
      tipo_registro = `Tipo de registro`,
      id_registro = `ID do registro`,
      data_inicio = `Data de início`,
      data_termino = `Data de término`,
      hora_inicio = `Hora de início`,
      hora_termino = `Hora de término`,
      origem = `Origem`,
      destino = `Destino`,
      forma_realizacao = `Forma de realização`,
      data_publicacao = `Data da publicação`,
      ultima_modificacao = `Última modificação`,
      assunto_compromisso = `Assunto do Compromisso`,
      local = `Local`,
      participantes = `Participantes`,
      vinculacoes = `Vinculações`,
      objetivos = `Objetivos`,
      detalhamento_audiencia = `Detalhamento da Audiência`,
      detalhes = `Detalhes`
    )
  
  cat("  -> Nome da entidade renomeado para 'IFS'\n")
  cat("  -> Valores 'ND' e vazios convertidos para NULL\n")
  cat("  -> Colunas convertidas para snake_case\n")
  
  # Converter datas para formato ISO (YYYY-MM-DD)
  colunas_data <- c("data_inicio", "data_termino", "data_publicacao", "ultima_modificacao")
  
  for (col in colunas_data) {
    if (col %in% colnames(resultado_final)) {
      tryCatch({
        # Apenas converte datas não-NA
        resultado_final <- resultado_final %>%
          mutate(!!col := if_else(
            is.na(!!sym(col)) | !!sym(col) == "",
            NA_character_,
            format(dmy(!!sym(col)), "%Y-%m-%d")
          ))
        cat(sprintf("  -> Coluna '%s' convertida para ISO (YYYY-MM-DD)\n", col))
      }, error = function(e) {
        cat(sprintf("  -> Aviso: Não foi possível converter '%s'\n", col))
      })
    }
  }
  
  # Remover espaços extras
  resultado_final <- resultado_final %>%
    mutate(across(where(is.character), str_trim))
  
  cat("  -> Espaços em branco extras removidos\n")
  
  # Limpar coluna participantes (remover prefixo padrão)
  if ("participantes" %in% colnames(resultado_final)) {
    resultado_final <- resultado_final %>%
      mutate(participantes = str_remove(participantes, "^Agentes públicos obrigados participantes:\\s*"))
    cat("  -> Prefixo removido da coluna 'participantes'\n")
  }
  
  cat("\n")
  
  # CRÍTICO: Garantir que todos os registros tenham as mesmas colunas
  # Define todas as colunas esperadas
  colunas_esperadas <- c(
    "nome", "cargo_funcao", "tipo_exercicio", "orgao_entidade",
    "tipo_registro", "id_registro", "data_inicio", "data_termino",
    "hora_inicio", "hora_termino", "origem", "destino",
    "forma_realizacao", "data_publicacao", "ultima_modificacao",
    "assunto_compromisso", "local", "participantes", "vinculacoes",
    "objetivos", "detalhamento_audiencia", "detalhes"
  )
  
  # Adiciona colunas faltantes com NA
  for (col in colunas_esperadas) {
    if (!col %in% colnames(resultado_final)) {
      resultado_final[[col]] <- NA_character_
      cat(sprintf("  -> Coluna '%s' adicionada com valores NA\n", col))
    }
  }
  
  # Reordena para ter sempre a mesma ordem
  resultado_final <- resultado_final %>%
    select(all_of(colunas_esperadas))
  
  cat("  -> Todas as colunas padronizadas e ordenadas\n")
  
  # Converter strings vazias para NA em TODAS as colunas
  # O write_json vai converter NA para null no JSON
  resultado_final <- resultado_final %>%
    mutate(across(everything(), ~if_else(. == "" | is.na(.), NA_character_, .)))
  
  cat("  -> Strings vazias e NAs padronizados para null\n\n")
  
  # 7. Salvar JSON (na = "null" converte NA para null no JSON)
  arquivo_json <- "IFS_agenda.json"
  write_json(resultado_final, arquivo_json, pretty = TRUE, auto_unbox = TRUE, na = "null")
  cat(sprintf("Resultados salvos em: %s\n\n", arquivo_json))
  
  # Exibir amostra
  cat("--- AMOSTRA DOS DADOS ---\n")
  print(head(resultado_final, 10))
  
  if (nrow(resultado_final) > 10) {
    cat(sprintf("\n... e mais %d registros.\n", nrow(resultado_final) - 10))
  }
  
  # 8. WEBHOOK - Enviar notificação para n8n
  cat("\n=== ATIVANDO WEBHOOK ===\n")
  webhook_url <- "https://n8n.rodslater.com/webhook/ifs_agenda"
  
  # Resumo por tipo de registro
  resumo_tipo <- resultado_final %>% 
    count(tipo_registro, sort = TRUE)
  
  # Resumo por forma de realização
  resumo_forma <- resultado_final %>%
    count(forma_realizacao, sort = TRUE) %>%
    head(5)
  
  payload <- list(
    status = "success",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    message = "Dados de agenda IFS atualizados com sucesso",
    total_registros = nrow(resultado_final),
    resumo_tipo_registro = resumo_tipo,
    resumo_forma_realizacao = resumo_forma,
    arquivo_atualizado = arquivo_json
  )
  
  tryCatch({
    cat("Enviando payload para webhook...\n")
    
    response <- POST(
      url = webhook_url,
      body = payload,
      encode = "json",
      add_headers("Content-Type" = "application/json"),
      timeout(30)
    )
    
    if (status_code(response) == 200) {
      cat("✅ Webhook n8n ativado com sucesso!\n")
      cat("Response:", content(response, "text"), "\n")
    } else {
      cat("❌ Erro ao ativar webhook. Status code:", status_code(response), "\n")
      cat("Response:", content(response, "text"), "\n")
    }
    
  }, error = function(e) {
    cat("❌ Erro ao chamar webhook:", e$message, "\n")
    
    tryCatch({
      cat("Tentando webhook simples (GET)...\n")
      response_get <- GET(webhook_url, timeout(30))
      if (status_code(response_get) == 200) {
        cat("✅ Webhook ativado com GET!\n")
      }
    }, error = function(e2) {
      cat("❌ Falha total ao ativar webhook:", e2$message, "\n")
    })
  })
  
} else {
  cat(sprintf("\nNenhum registro encontrado para '%s'\n", entidade_alvo))
  
  # WEBHOOK para caso de erro
  webhook_url <- "https://n8n.rodslater.com/webhook/ifs_agenda"
  
  payload_erro <- list(
    status = "error",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    message = "Falha no processamento dos dados de agenda IFS",
    total_registros = 0,
    erro = "Nenhum registro encontrado para a entidade especificada"
  )
  
  tryCatch({
    response <- POST(
      url = webhook_url,
      body = payload_erro,
      encode = "json",
      add_headers("Content-Type" = "application/json"),
      timeout(30)
    )
    
    if (status_code(response) == 200) {
      cat("✅ Webhook de erro enviado com sucesso!\n")
    }
  }, error = function(e) {
    cat("❌ Erro ao enviar webhook de erro:", e$message, "\n")
  })
}

# 9. Limpar arquivos temporários
cat("\nLimpando arquivos temporários...\n")
unlink(arquivo_zip)
unlink(dir_extracao, recursive = TRUE)
cat("Processamento de agenda concluído!\n")
