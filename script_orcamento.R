library(dplyr)
library(purrr)
library(downloader)
library(lubridate)
library(data.table)
library(jsonlite)
library(httr)

# Função para baixar e processar dados de orçamento de um ano específico
baixar_processar_orcamento_ano <- function(ano) {
  cat("Processando orçamento do ano:", ano, "\n")
  
  # URL para o ano específico
  url <- paste0("https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa/", ano)
  
  # Nome do arquivo zip
  zip_file <- paste0("dataset_orcamento_", ano, ".zip")
  csv_file <- paste0(ano, "_OrcamentoDespesa.csv")
  
  # Download do arquivo
  tryCatch({
    download(url, dest = zip_file, mode = "wb")
    unzip(zip_file)
    
    # Leitura do arquivo CSV
    orcamento <- fread(csv_file, encoding = 'Latin-1')
    
    # Processamento dos dados do IFS
    IFS_orcamento_ano <- orcamento %>%
      filter(`CÓDIGO ÓRGÃO SUBORDINADO` == 26423) %>%   # é inteiro, não string
      select(
        ano = `EXERCÍCIO`,
        orgao = `NOME ÓRGÃO SUBORDINADO`,
        funcao = `NOME FUNÇÃO`,
        subfuncao = `NOME SUBFUNÇÃO`,
        programa = `NOME PROGRAMA ORÇAMENTÁRIO`,
        acao = `NOME AÇÃO`,
        cat_economica = `NOME CATEGORIA ECONÔMICA`,
        grupo_despesa = `NOME GRUPO DE DESPESA`,
        elemento_despesa = `NOME ELEMENTO DE DESPESA`,
        orc_inicial = `ORÇAMENTO INICIAL (R$)`,
        orc_atualizado = `ORÇAMENTO ATUALIZADO (R$)`,
        orc_empenhado = `ORÇAMENTO EMPENHADO (R$)`,
        orc_realizado = `ORÇAMENTO REALIZADO (R$)`
      )
    
    # Limpeza dos arquivos temporários
    arquivos_para_remover <- c(zip_file, csv_file)
    
    # Remove apenas os arquivos que existem
    arquivos_existentes <- arquivos_para_remover[file.exists(arquivos_para_remover)]
    if(length(arquivos_existentes) > 0) {
      file.remove(arquivos_existentes)
    }
    
    cat("Ano", ano, "processado com sucesso.", nrow(IFS_orcamento_ano), "registros orçamentários encontrados.\n")
    return(IFS_orcamento_ano)
    
  }, error = function(e) {
    cat("Erro ao processar ano", ano, ":", e$message, "\n")
    return(NULL)
  })
}

# Anos para processar (ano atual e 4 anteriores)
ano_atual <- year(Sys.Date())
anos <- (ano_atual - 4):ano_atual

cat("Baixando dados de orçamento do IFS para os anos:", paste(anos, collapse = ", "), "\n\n")

# Processar cada ano
lista_dataframes_orcamento <- map(anos, baixar_processar_orcamento_ano)

# Remove elementos NULL (anos que falharam)
lista_dataframes_orcamento <- lista_dataframes_orcamento[!sapply(lista_dataframes_orcamento, is.null)]

# Combina todos os dataframes
if(length(lista_dataframes_orcamento) > 0) {
  IFS_orcamento_completo <- bind_rows(lista_dataframes_orcamento)
  
  cat("\n=== RESUMO FINAL ===\n")
  cat("Total de registros orçamentários IFS encontrados:", nrow(IFS_orcamento_completo), "\n")
  cat("Anos processados:", paste(unique(IFS_orcamento_completo$ano), collapse = ", "), "\n")
  
  # Resumo por ano
  cat("\nRegistros por ano:\n")
  resumo_por_ano <- IFS_orcamento_completo %>% 
    count(ano, sort = TRUE)
  print(resumo_por_ano)
  
  # Resumo financeiro por ano
  cat("\nResumo financeiro por ano (valores em R$):\n")
  resumo_financeiro <- IFS_orcamento_completo %>%
    group_by(ano) %>%
    summarise(
      total_inicial = sum(orc_inicial, na.rm = TRUE),
      total_atualizado = sum(orc_atualizado, na.rm = TRUE),
      total_empenhado = sum(orc_empenhado, na.rm = TRUE),
      total_realizado = sum(orc_realizado, na.rm = TRUE),
      .groups = 'drop'
    ) %>%
    mutate(across(starts_with("total_"), ~ format(.x, big.mark = ".", decimal.mark = ",", scientific = FALSE)))
  print(resumo_financeiro)
  
  # Visualização das primeiras linhas
  cat("\nPrimeiras linhas do dataset completo:\n")
  print(head(IFS_orcamento_completo))
  
  # Salva o arquivo JSON
  write_json(IFS_orcamento_completo, "IFS_orcamento.json", pretty = TRUE, auto_unbox = TRUE)
  
  # WEBHOOK - Enviar notificação para n8n
  webhook_url <- "https://n8n.rodslater.com/webhook/ifs_orcamento"
  
  # Recalcular resumo financeiro para o payload (sem formatação)
  resumo_financeiro_raw <- IFS_orcamento_completo %>%
    group_by(ano) %>%
    summarise(
      total_inicial = sum(orc_inicial, na.rm = TRUE),
      total_atualizado = sum(orc_atualizado, na.rm = TRUE),
      total_empenhado = sum(orc_empenhado, na.rm = TRUE),
      total_realizado = sum(orc_realizado, na.rm = TRUE),
      .groups = 'drop'
    )
  
  payload <- list(
    status = "success",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    message = "Dados de orçamento IFS atualizados com sucesso",
    total_registros = nrow(IFS_orcamento_completo),
    anos_processados = unique(IFS_orcamento_completo$ano),
    resumo_financeiro = resumo_financeiro_raw,
    registros_por_ano = resumo_por_ano,
    arquivo_atualizado = "IFS_orcamento.json"
  )
  
  tryCatch({
    cat("Ativando webhook n8n...\n")
    
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
  cat("Nenhum dado foi processado com sucesso.\n")
  IFS_orcamento_completo <- NULL
  
  # WEBHOOK para caso de erro
  webhook_url <- "https://n8n.rodslater.com/webhook/ifs_orcamento"
  
  payload_erro <- list(
    status = "error",
    timestamp = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    message = "Falha no processamento dos dados de orçamento IFS",
    total_registros = 0,
    anos_tentados = anos,
    erro = "Nenhum arquivo foi processado com sucesso"
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

# Limpeza final - remove qualquer arquivo temporário restante
arquivos_temp <- list.files(pattern = "dataset_orcamento_.*\\.zip|\\d{4}_OrcamentoDespesa\\.csv")
if(length(arquivos_temp) > 0) {
  file.remove(arquivos_temp)
}

cat("Processamento de orçamento concluído!\n")
