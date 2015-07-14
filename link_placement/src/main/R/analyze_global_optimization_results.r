.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/results/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG', Sys.getenv('HOME'))

results_chain <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_CHAIN.tsv.gz', DATADIR)),
                            header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_chain) <- paste(results_chain$src, results_chain$tgt)
results_tree <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_TREE.tsv.gz', DATADIR)),
                           header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_tree) <- paste(results_tree$src, results_tree$tgt)
results_coins <- read.table(pipe(sprintf('gunzip -c %s/link_placement_results_COINS.tsv.gz', DATADIR)),
                            header=TRUE, sep='\t', comment.char='', encoding='UTF-8', quote='', stringsAsFactors=FALSE)
rownames(results_coins) <- paste(results_coins$src, results_coins$tgt)


chain_objective <- function(r) {
  (r$source_transition_count_before * r$pst_hat / (r$pst_hat + r$avg_nonleaf_source_deg_before))
}


Ns <- results_chain$source_count_before
names(Ns) <- results_chain$src
Ns <- Ns[unique(names(Ns))]
plot(sort(Ns), length(Ns):1, type='l', log='', xlim=c(1,1e4))
plot(sort(Ns), length(Ns):1, type='l', log='xy')
plot(sort(Ns[Ns<1e4]), sum(Ns<1e4):1, type='l', log='xy')

K <- 1e4
max_Ns <- 1e4
plot(cumsum(results_coins$marg_gain[results_coins$source_count_before <= max_Ns][1:K]), type='l')
lines(cumsum(results_tree$marg_gain[results_tree$source_count_before <= max_Ns][1:K]), type='l', col='red')
lines(cumsum(results_chain$marg_gain[results_chain$source_count_before <= max_Ns][1:K]), type='l', col='green')

sapply(1:10000, function(i) length(unique(r$src[1:i])))

r <- results_chain
idx <- which(r$src==names(Ns)[1])
head(cbind(r$marg_gain, chain_objective(r))[idx,],50)



# Overlap of objectives

jaccard <- function(s1, s2) length(intersect(s1, s2)) / length(union(s1, s2))

K <- 10000
jacc <- sapply(1:K, function(i) jaccard(rownames(results_chain)[1:i], rownames(results_coins)[1:i]))
plot(jacc, type='l')


plot(cumsum(results_chain$chain_marg_gain[1:1e4]), type='l', col='green', log='')
lines(cumsum(results_coins$chain_marg_gain[1:1e4]), type='l', col='red')


K <- 1e4
plot(cumsum(results_chain$marg_gain[1:K]), type='l')

