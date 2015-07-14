.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/results/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG', Sys.getenv('HOME'))

pr <- read.table(pipe(sprintf('gunzip -c %s/prec_recall.tsv.gz', DATADIR)), header=TRUE, quote='', sep='\t')
K <- 1e4
top <- pr[1:K,]

#lines(top$p_baseline_mean_recall, top$p_baseline_mean_precision, col='green')
#lines(top$p_baseline_median_recall, top$p_baseline_median_precision, col='blue')

# Precision/recall.
if (save_plots) pdf(sprintf('%s/prec_rec.pdf', PLOTDIR), width=3.2, height=2, pointsize=6,
                    family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 0.8, 0.8))
plot(top$p_indirect_recall, top$p_indirect_precision, type='l', log='xy', bty='n', ylim=c(0.2,1),
     col='black', xlab='', ylab='', xlim=c(min(top$p_indirect_recall[top$p_indirect_recall>0]), 1))
lines(top$p_transitive_recall, top$p_transitive_precision, col='red')
mtext('Recall', side=1, line=2.4)
mtext('Precision', side=2, line=2.4)
legend('bottomleft', legend=c('Indirect-path probability (empirical)',
                              'Indirect-path probability (random walks)'), bty='n',
       lty=1, col=c('black', 'red'))
if (save_plots) dev.off()

# Prec@k.
if (save_plots) pdf(sprintf('%s/prec_at_k.pdf', PLOTDIR), width=3.2, height=2, pointsize=6,
                    family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 0.8, 0.8))
plot(1:K, top$p_indirect_precision, type='l', log='xy', bty='n', ylim=c(0.2,1), col='black',
     xlab='', ylab='')
lines(1:K, top$p_transitive_precision, col='red')
mtext(expression(paste('Rank ', italic(k))), side=1, line=2.4)
mtext(expression(paste('Precision@', italic(k))), side=2, line=2.4)
legend('bottomleft', legend=c('Indirect-path probability (empirical)',
                            'Indirect-path probability (random walks)'), bty='n',
       lty=1, col=c('black', 'red'))
if (save_plots) dev.off()


plot(pr$p_baseline_mean_precision, col='green', type='l', log='xy')
