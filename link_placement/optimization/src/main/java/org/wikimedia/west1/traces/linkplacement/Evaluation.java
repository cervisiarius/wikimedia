package org.wikimedia.west1.traces.linkplacement;

import java.io.IOException;
import java.io.PrintStream;

public class Evaluation {

	public static void main(String[] args) throws IOException {
		SubmodularLinkPlacement submodular;
		CumulativeScoreLinkPlacement cumulative;
		ScoreLinkPlacement simple;

		System.out.println("------------------------------ WIKIPEDIA ------------------------------");
		PrintStream out = new PrintStream(LinkPlacement.DATADIR_WIKIPEDIA
		    + "link_placement_results.tsv");
		out.format("%s\t%s\t%s\t%s\t%s\t%s\n", "n", "simple", "cumulative", "submodular", "online_bound",
		    "offline_bound");

		int[] N_wiki = { 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 20000, 50000 };
		submodular = new SubmodularLinkPlacement(LinkPlacement.DATADIR_WIKIPEDIA);
		cumulative = new CumulativeScoreLinkPlacement(LinkPlacement.DATADIR_WIKIPEDIA);
		simple = new ScoreLinkPlacement(LinkPlacement.DATADIR_WIKIPEDIA);
		for (int n : N_wiki) {
			System.out.println(n);

			// Submodular.
			submodular.reset();
			submodular.placeLinks(n, true);
			float value_submodular = submodular.evaluate();
			float onlineBound = submodular.getUpperBound(n, value_submodular);
			double offlineBound = value_submodular / (1 - 1 / Math.E);

			// Cumulative score.
			cumulative.reset();
			cumulative.placeLinks(n, true);
			float value_cumulative = cumulative.evaluate();

			// Simple score.
			simple.reset();
			simple.placeLinks(n, true);
			float value_simple = simple.evaluate();

			// Summarize results.
			out.format("%s\t%s\t%s\t%s\t%s\t%s\n", n, value_simple, value_cumulative, value_submodular,
			    onlineBound, offlineBound);
		}
		out.close();

		System.out.println("------------------------------ SIMTK ------------------------------");
		out = new PrintStream(LinkPlacement.DATADIR_SIMTK + "link_placement_results.tsv");
		out.format("%s\t%s\t%s\t%s\t%s\t%s\n", "n", "simple", "cumulative", "submodular", "online_bound",
		    "offline_bound");

		int[] N_simtk = { 10, 20, 50, 100, 200, 500 };
		submodular = new SubmodularLinkPlacement(LinkPlacement.DATADIR_SIMTK);
		cumulative = new CumulativeScoreLinkPlacement(LinkPlacement.DATADIR_SIMTK);
		simple = new ScoreLinkPlacement(LinkPlacement.DATADIR_SIMTK);
		for (int n : N_simtk) {
			System.out.println(n);

			// Submodular.
			submodular.reset();
			submodular.placeLinks(n, true);
			float value_submodular = submodular.evaluate();
			float onlineBound = submodular.getUpperBound(n, value_submodular);
			double offlineBound = value_submodular / (1 - 1 / Math.E);

			// Cumulative score.
			cumulative.reset();
			cumulative.placeLinks(n, true);
			float value_cumulative = cumulative.evaluate();

			// Simple score.
			simple.reset();
			simple.placeLinks(n, true);
			float value_simple = simple.evaluate();

			// Summarize results.
			out.format("%s\t%s\t%s\t%s\t%s\t%s\n", n, value_simple, value_cumulative, value_submodular,
			    onlineBound, offlineBound);
		}
		out.close();
	}

}
