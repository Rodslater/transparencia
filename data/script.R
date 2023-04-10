library(dplyr)
library(downloader)
library(lubridate)

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

saveRDS(viagens_IFS, paste0('data/',Sys.Date(),'_viagens_IFS','.rds'))


###### Orçamento
url2 <- "https://portaldatransparencia.gov.br/download-de-dados/orcamento-despesa/2023"
download(url2, dest="dataset.zip", mode="wb") 
unzip ("dataset.zip")

orcamento  <- read.csv2("2023_OrcamentoDespesa.zip.csv", dec =",", fileEncoding='latin1')

file.remove(c('dataset.zip', '2023_OrcamentoDespesa.zip.csv'))

orcamento_IFS <- orcamento %>% 
  filter(CÓDIGO.ÓRGÃO.SUBORDINADO == "26423") %>%
  select(exercicio=EXERCÍCIO, orgao=NOME.ÓRGÃO.SUBORDINADO, funcao=NOME.FUNÇÃO, subfuncao=NOME.SUBFUNÇÃO, programa=NOME.PROGRAMA.ORÇAMENTÁRIO, acao=NOME.AÇÃO, cat_economica=NOME.CATEGORIA.ECONÔMICA, grupo_despesa=NOME.GRUPO.DE.DESPESA, elemento_despesa=NOME.ELEMENTO.DE.DESPESA, orc_inicial=ORÇAMENTO.INICIAL..R.., orc_atualizado=ORÇAMENTO.ATUALIZADO..R.., orc_empenhado=ORÇAMENTO.EMPENHADO..R.., orc_realizado=ORÇAMENTO.REALIZADO..R..)
  

saveRDS(orcamento_IFS, paste0('data/',Sys.Date(),'_orcamento_IFS','.rds'))



# write the README.md file

# create table to add on README
table <- viagens_IFS |>
  knitr::kable()

# Write the content on README
paste0(
  "# Repositories from quarto-dev
Updated with GitHub Actions in ",
format(Sys.Date(), '%b %d %Y'),
".
<hr> \n
",
paste(table, collapse = "\n")
) |> writeLines("data/README.md")

print("The end! Congrats!")
