.default_par <- par(no.readonly=TRUE)

# Set this to FALSE if you don't want to save plots to PDFs.
save_plots <- TRUE

DATADIR <- sprintf('%s/wikimedia/trunk/data/link_placement/', Sys.getenv('HOME'))
PLOTDIR <- sprintf('%s/snap-papers/2015/west1-ashwinp-wmf/FIG', Sys.getenv('HOME'))

data <- read.table(sprintf('%s/nav_struct_degree_analysis.tsv', DATADIR), header=TRUE, quote='', sep='\t')

data$pstop_before <- data$source_terminal_count_before / data$source_count_before
data$pstop_after <- data$source_terminal_count_after / data$source_count_after

data$navdeg_before <- data$transitions_before / data$source_count_before
data$navdeg_after <- data$transitions_after / data$source_count_after

data$navdeg1_before <- data$transitions_before / data$source_continuation_count_before
data$navdeg1_after <- data$transitions_after / data$source_continuation_count_after

quant_structdeg_before <- quantile(data$struct_deg_before, seq(0,1,.1))
b <- c(1,10,100,Inf)
breaks_structdeg_delta <- c(rev(-b), 0, b)

width <- 1.68
height <- 1.5

# Stopping prob as fct of struct deg.
groups <- split(data$pstop_before, cut(data$struct_deg_before, breaks=quant_structdeg_before, right=FALSE))
if (save_plots) pdf(sprintf('%s/structdeg_vs_pstop.pdf', PLOTDIR), width=width, height=height, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(5, 3.4, 0.8, 0.8))
boxplot(groups, notch=FALSE, names=names(groups), outline=FALSE, xlab='', ylab='', las=2)
mtext('Struct. deg.', side=1, line=4)
mtext('Stopping probability', side=2, line=2.4)
if (save_plots) dev.off()

# Nav deg as fct of struct deg (conditioned on nav deg > 0).
groups <- split(data$navdeg1_before, cut(data$struct_deg_before, breaks=quant_structdeg_before, right=FALSE))
if (save_plots) pdf(sprintf('%s/structdeg_vs_navdeg.pdf', PLOTDIR), width=width, height=height, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(5, 3.4, 0.8, 0.8))
boxplot(groups, notch=FALSE, names=names(groups), outline=FALSE, xlab='', ylab='', las=2)
mtext('Struct. deg.', side=1, line=4)
mtext('Nav. deg.', side=2, line=2.4)
if (save_plots) dev.off()

# Stopping prob delta as fct of struct deg delta.
groups <- split((data$pstop_after - data$pstop_before)/data$pstop_before,
                cut(data$struct_deg_after - data$struct_deg_before, breaks=breaks_structdeg_delta, right=FALSE))
if (save_plots) pdf(sprintf('%s/structdeg-delta_vs_pstop-delta.pdf', PLOTDIR), width=width, height=height, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(5, 3.4, 0.8, 0.8))
boxplot(groups, notch=FALSE, names=names(groups), outline=FALSE, xlab='', ylab='', las=2)
abline(h=0, col='red')
mtext('Struct. deg. diff.', side=1, line=4)
mtext('Stopping prob. diff.', side=2, line=2.4)
if (save_plots) dev.off()

# Nav deg delta as fct of struct deg delta (conditioned on nav deg > 0).
groups <- split((data$navdeg1_after - data$navdeg1_before)/data$navdeg1_before,
                cut(data$struct_deg_after - data$struct_deg_before, breaks=breaks_structdeg_delta, right=FALSE))
if (save_plots) pdf(sprintf('%s/structdeg-delta_vs_navdeg1-delta.pdf', PLOTDIR), width=width, height=height, pointsize=6, family='Helvetica', useDingbats=FALSE)
par(mar=c(5, 3.4, 0.8, 0.8))
boxplot(groups, notch=FALSE, names=names(groups), outline=FALSE, xlab='', ylab='', las=2)
abline(h=0, col='red')
mtext('Struct. deg. diff.', side=1, line=4)
mtext('Nav. deg. diff.', side=2, line=2.4)
if (save_plots) dev.off()




data1 <- data[data$struct_deg_after - data$struct_deg_before == 1,]
groups <- split((data1$navdeg1_after - data1$navdeg1_before) / data1$navdeg1_before,
                cut(data1$struct_deg_before, breaks=quantile(data1$struct_deg_before, seq(0,1,.1)), right=FALSE))
#if (save_plots) pdf(sprintf('%s/structdeg-delta_vs_navdeg1-delta.pdf', PLOTDIR), width=width, height=height, pointsize=6, family='Helvetica', useDingbats=FALSE)
boxplot(groups, notch=FALSE, names=names(groups), outline=FALSE, xlab='Structural degree difference', ylab='Nav. deg. diff.', las=2)
abline(h=0, col='red')
#if (save_plots) dev.off()
plot(unlist(lapply(groups, function(x) mean(x,na.rm=TRUE))))
