library(xtable)
library(boot)

.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- FALSE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/results/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG', Sys.getenv('HOME'))

####################################################################
# Precision and recall
####################################################################

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
if (save_plots) pdf(sprintf('%s/prec_at_k.pdf', PLOTDIR), width=1.68, height=1.5, pointsize=6,
                    family='Helvetica', useDingbats=FALSE)
par(mar=c(3.4, 3.4, 0.8, 0.8))
plot(1:K, top$p_transitive_precision, col='#CC79A7', type='l', log='x', bty='n', ylim=c(0,1), xlab='', ylab='')
lines(1:K, top$p_indirect_precision, col='black')
lines(1:K, top$p_search_precision, col='#009E73')
mtext(expression(paste('Rank ', italic(k))), side=1, line=2.4)
mtext(expression(paste('Precision@', italic(k))), side=2, line=2.4)
legend('bottomleft', legend=c('Path proportion', 'Search proportion', 'Random walks'), bty='n',
       lty=1, col=c('black', '#009E73', '#CC79A7'))
if (save_plots) dev.off()

# The baseline plot. Too low to make it into the plot in the paper.
plot(pr$p_baseline_mean_precision, col='green', type='l', log='xy')


####################################################################
# Mean absolute error etc.
####################################################################

# plog2p <- function(p) { x <- p*log2(p); x[is.nan(x)] <- 0; x }
# kl <- function(p, q) sum(plog2p(p) - p*log2(q))
# js <- function(p, q) {
#   # Ignore items where both methods predict 0, since JS divergence is not defined here.
#   idx <- which(p + q > 0)
#   p <- p[idx]
#   q <- q[idx]
#   (kl(p, p/2 + q/2) + kl(q, p/2 + q/2)) / 2
# }
# plog2q <- function(p, q) { x <- p*log2(q); x[is.nan(x)] <- 0; x }
# logl <- function(p, q) {
#   idx <- which(!(p == 0 & q > 0 | p > 0 & q == 0 | p == 1 & q < 1 | p < 1 & q == 1))
#   p <- p[idx]
#   q <- q[idx]
#   -sum(plog2q(p, q) + plog2q(1-p, 1-q))
# }

pred_all <- read.table(sprintf('%s/p_eval_masterfile.tsv', DATADIR), header=TRUE, quote='', sep='\t')
pred_all$zero <- 0
pred <- pred_all[,c(22,12,20,21,15,10,23)]

nboot <- 1000
boot_ci <- function(data, f) {
  boot_samples <- apply(data[,-1], 2, function(x) boot(x, function(xx,j) f(data$pst_groundtruth[j], xx[j]), nboot, ncpus=8))
  do.call(rbind, lapply(boot_samples, function(s) {
    b <- boot.ci(s, type='basic')
    c(b$basic[4], b$t0, b$basic[5])
  }))
}

# cols: lower 95% CI, estimated value, upper 95% CI
estim_MAE <- boot_ci(pred, function(pst, psthat) mean(abs(pst - psthat)))
estim_Pearson <- boot_ci(pred[,-7], function(pst, psthat) cor(pst, psthat, method='pearson'))
estim_Spearman <- boot_ci(pred[,-7], function(pst, psthat) cor(pst, psthat, method='spearman'))
estim_Pearson <- rbind(estim_Pearson, c(NA, NA, NA)); rownames(estim_Pearson)[nrow(estim_Pearson)] <- 'zero'
estim_Spearman <- rbind(estim_Spearman, c(NA, NA, NA)); rownames(estim_Spearman)[nrow(estim_Spearman)] <- 'zero'

# The mean between (y-lower) and (upper-y)
delta_MAE <- rowMeans(cbind(estim_MAE[,2] - estim_MAE[,1], estim_MAE[,3] - estim_MAE[,2]))
delta_Pearson <- rowMeans(cbind(estim_Pearson[,2] - estim_Pearson[,1], estim_Pearson[,3] - estim_Pearson[,2]))
delta_Spearman <- rowMeans(cbind(estim_Spearman[,2] - estim_Spearman[,1], estim_Spearman[,3] - estim_Spearman[,2]))

stringify <- function(estim, delta, digits) sprintf(sprintf('%%.%df (+-%%.%df)', digits, digits), estim[,2], delta)
tbl <- cbind(stringify(estim_MAE, delta_MAE, 4),
             stringify(estim_Pearson, delta_Pearson, 2),
             stringify(estim_Spearman, delta_Spearman, 2))
rownames(tbl) <- c('Path prop.', 'Search prop.', 'P&S prop.', 'Rand. walks', 'Mean baseln.', 'Zero baseln.')
colnames(tbl) <- c('MAE', 'Pearson', 'Spearman')

cat(gsub('\\(\\$\\\\pm\\$NA\\)', '',
         gsub('-', '$-$',
              gsub('\\+-', '$\\\\pm$', print(xtable(tbl), floating=FALSE)))))





# OLD (without CIs)
MAE <- colMeans(abs(pred[,-1] - pred$pst_groundtruth))
Pearson <- cor(pred, method='pearson')[,1][-1]
Spearman <- cor(pred, method='spearman')[,1][-1]

perf <- cbind(MAE, Pearson, Spearman)
perf <- perf[order(Pearson, decreasing=TRUE),]
perf

print(xtable(perf, digits=c(1,5,3,3)), floating=FALSE)
