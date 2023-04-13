library(dplyr)
library(downloader)
library(lubridate)

## Execução das despesas
datas <- c(202301:202312, 202401:202412)
for(i in seq_along(datas)) {
  try({url <- paste0('https://portaldatransparencia.gov.br/download-de-dados/despesas-execucao/', datas[i]) #Try passa pra o próximo em caso de erro.
  arquivo <- sprintf("dataset_%s.zip", datas[i])
  download(url, dest=arquivo, mode="wb") 
  unzip (arquivo)
  file.remove(arquivo)
  })
}

despesas <- import_list(dir(pattern = ".csv"), rbind = TRUE)
despesas_IFS <- despesas %>% 
  filter(`Código Órgão Subordinado` == "26423")

saveRDS(despesas_IFS, 'data/despesas_IFS.rds')

arquivos_csv <- dir(pattern = ".csv")
if (file.exists(arquivos_csv)) {
  file.remove(arquivos_csv)
}


###### Viagens
url <- "https://portaldatransparencia.gov.br/download-de-dados/viagens/2023"
download(url, dest="dataset.zip", mode="wb") 
unzip ("dataset.zip")

viagens <- read.csv2("2023_Viagem.csv", dec =",", fileEncoding='latin1')
file.remove(c('dataset.zip', '2023_Pagamento.csv', '2023_Passagem.csv', '2023_Trecho.csv', '2023_Viagem.csv'))

viagens_IFS <- viagens %>%
  filter(Código.órgão.solicitante == "26423",
         Situação == 'Realizada') %>% 
  select(nome=Nome, cargo=Cargo,inicio=Período...Data.de.início, fim=Período...Data.de.fim, destino=Destinos, valor_diaria=Valor.diárias, valor_passagem=Valor.passagens, urgencia=Viagem.Urgente) %>% 
  mutate(inicio= dmy(inicio), fim= dmy(fim))

saveRDS(viagens_IFS, 'data/viagens_IFS.rds')


###### Orçamento
url2 <- "https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa/2023"
download(url2, dest="dataset.zip", mode="wb") 
unzip ("dataset.zip")

orcamento  <- read.csv2("2023_OrcamentoDespesa.zip.csv", dec =",", fileEncoding='latin1')

file.remove(c('dataset.zip', '2023_OrcamentoDespesa.zip.csv'))

orcamento_IFS <- orcamento %>% 
  filter(CÓDIGO.ÓRGÃO.SUBORDINADO == "26423") %>%
  select(exercicio=EXERCÍCIO, orgao=NOME.ÓRGÃO.SUBORDINADO, funcao=NOME.FUNÇÃO, subfuncao=NOME.SUBFUNÇÃO, programa=NOME.PROGRAMA.ORÇAMENTÁRIO, acao=NOME.AÇÃO, cat_economica=NOME.CATEGORIA.ECONÔMICA, grupo_despesa=NOME.GRUPO.DE.DESPESA, elemento_despesa=NOME.ELEMENTO.DE.DESPESA, orc_inicial=ORÇAMENTO.INICIAL..R.., orc_atualizado=ORÇAMENTO.ATUALIZADO..R.., orc_empenhado=ORÇAMENTO.EMPENHADO..R.., orc_realizado=ORÇAMENTO.REALIZADO..R..)
  
saveRDS(orcamento_IFS, 'data/orcamento_IFS.rds')


###### Receitas
url3 <- "https://portaldatransparencia.gov.br/download-de-dados/receitas/2023"
download(url3, dest="dataset.zip", mode="wb") 
unzip ("dataset.zip")

receitas  <- read.csv2("2023_Receitas.csv", dec =",", fileEncoding='latin1')

file.remove(c('dataset.zip', '2023_Receitas.csv'))

receitas_IFS <- receitas %>% 
  filter(CÓDIGO.ÓRGÃO == "26423") %>%
    select(exercicio=ANO.EXERCÍCIO, orgao=NOME.ÓRGÃO, ug=CÓDIGO.UNIDADE.GESTORA, cat_economica=CATEGORIA.ECONÔMICA, origem=ORIGEM.RECEITA, especie=ESPÉCIE.RECEITA, detalhamento=DETALHAMENTO, rec_prevista=VALOR.PREVISTO.ATUALIZADO, rec_lancada=VALOR.LANÇADO, rec_realizada=VALOR.REALIZADO, data=DATA.LANÇAMENTO)

receitas_IFS <- receitas_IFS %>% 
  mutate(data = dmy(data))

saveRDS(receitas_IFS, 'data/receitas_IFS.rds')
