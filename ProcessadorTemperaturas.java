

import java.util.concurrent.*;
import java.util.*;
import java.io.*;
import java.text.DecimalFormat;

public class ProcessadorTemperaturas {

    public static void main(String[] args) throws InterruptedException {

        int numRodadas = 10; 
        int numThreads = 320;
        List<Long> temposExecucao = new ArrayList<>(); 

        for (int rodada = 1; rodada <= numRodadas; rodada++) {
            long inicioTempo = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            
            List<File> listaArquivos = obterArquivosCSV("./temperaturas_cidades/temperaturas_cidades");

            if (listaArquivos.isEmpty()) {
                System.out.println("Nenhum arquivo CSV encontrado para processar.");
                return;
            }

            int tamanhoGrupo = Math.max(1, listaArquivos.size() / numThreads); 
            for (int i = 0; i < numThreads; i++) {
                int inicio = i * tamanhoGrupo;
                int fim = (i == numThreads - 1) ? listaArquivos.size() : Math.min(inicio + tamanhoGrupo, listaArquivos.size());

                if (inicio >= listaArquivos.size()) break;

                List<File> subLista = listaArquivos.subList(inicio, fim);
                executor.execute(new TarefaProcessamento(subLista));
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

            // Medir o tempo do fim da rodada e armazenar
            long fimTempo = System.currentTimeMillis();
            long tempoRodada = fimTempo - inicioTempo;
            temposExecucao.add(tempoRodada);

            System.out.println("Tempo de execucao da rodada " + rodada + ": " + tempoRodada + " ms");
        }

        long somaTempos = 0;
        for (long tempo : temposExecucao) {
            somaTempos += tempo;
        }
        long tempoMedio = somaTempos / numRodadas;

        System.out.println("Tempo medio de execucao: " + tempoMedio + " ms");

        salvarTemposExecucao(temposExecucao, tempoMedio, "versao_experimento.txt");
    }

    public static List<File> obterArquivosCSV(String diretorio) {
        List<File> arquivosCSV = new ArrayList<>();
        File pasta = new File(diretorio);

        if (pasta.exists() && pasta.isDirectory()) {
            File[] arquivos = pasta.listFiles((dir, nome) -> nome.toLowerCase().endsWith(".csv")); 
            if (arquivos != null) {
                arquivosCSV.addAll(Arrays.asList(arquivos)); 
            }
        } else {
            System.out.println("Diretorio nao encontrado: " + diretorio);
        }

        return arquivosCSV;
    }

    public static void salvarTemposExecucao(List<Long> tempos, long tempoMedio, String nomeArquivo) {
        try {

            File arquivoSaida = new File(nomeArquivo);

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(arquivoSaida))) {
                for (int i = 0; i < tempos.size(); i++) {
                    writer.write("Tempo da rodada " + (i + 1) + ": " + tempos.get(i) + " ms\n");
                }
                writer.write("Tempo medio de execucao: " + tempoMedio + " ms\n");
            }

            System.out.println("Tempos de execucao salvos em " + nomeArquivo);

        } catch (IOException e) {
            System.out.println("Erro ao salvar os tempos de execucao: " + e.getMessage());
        }
    }
}

class TarefaProcessamento implements Runnable {
    private List<File> arquivos;
    private DecimalFormat df = new DecimalFormat("#.###"); 
    public TarefaProcessamento(List<File> arquivos) {
        this.arquivos = arquivos;
    }

    @Override
    public void run() {
        for (File arquivo : arquivos) {
            processarArquivoCSV(arquivo);
        }
    }

    private void processarArquivoCSV(File arquivo) {
        Map<Integer, List<Double>> temperaturasPorMes = new HashMap<>();

        for (int mes = 1; mes <= 12; mes++) {
            temperaturasPorMes.put(mes, new ArrayList<>());
        }

        try (BufferedReader br = new BufferedReader(new FileReader(arquivo))) {
            String linha;
            boolean primeiraLinha = true;

            while ((linha = br.readLine()) != null) {
                if (primeiraLinha) {
                    primeiraLinha = false;
                    continue;
                }

                String[] valores = linha.split(",");

                if (valores.length < 6) {
                    System.out.println("Linha invalida no arquivo: " + arquivo.getName() + " -> " + linha);
                    continue;
                }

                try {
                    int mes = Integer.parseInt(valores[2]); 
                    double temperatura = Double.parseDouble(valores[5]); 
                    temperaturasPorMes.get(mes).add(temperatura);
                } catch (NumberFormatException e) {
                    System.out.println("Erro ao converter mes ou temperatura no arquivo: " + arquivo.getName() + " -> " + linha);
                }
            }

            StringBuilder resultado = new StringBuilder();
            resultado.append("Arquivo: ").append(arquivo.getName()).append("\n");

            for (int mes = 1; mes <= 12; mes++) {
                List<Double> temperaturas = temperaturasPorMes.get(mes);

                if (!temperaturas.isEmpty()) {
                    double media = calcularMedia(temperaturas);
                    double maxima = Collections.max(temperaturas);
                    double minima = Collections.min(temperaturas);


                    resultado.append("Mes: ").append(mes).append("\n");
                    resultado.append("Temperatura Media: ").append(df.format(media)).append("\n");
                    resultado.append("Temperatura Maxima: ").append(df.format(maxima)).append("\n");
                    resultado.append("Temperatura Minima: ").append(df.format(minima)).append("\n");
                    resultado.append("------------------------------------\n");
                }
            }

            System.out.println(resultado);

        } catch (IOException e) {
            System.out.println("Erro ao ler o arquivo CSV: " + arquivo.getName());
            e.printStackTrace();
        }
    }

    private double calcularMedia(List<Double> temperaturas) {
        double soma = 0.0;
        for (double temp : temperaturas) {
            soma += temp;
        }
        return soma / temperaturas.size();
    }
}