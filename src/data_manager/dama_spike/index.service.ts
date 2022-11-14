import { Context } from "moleculer";

export default {
  name: "dama/spike",

  actions: {
    async testDbTransaction(ctx: Context) {
      const transaction = await ctx.call("dama_db.createTransaction");

      const transactionCtx = ctx.copy();
      transactionCtx.meta = {
        ...ctx.meta,
        transactionId: transaction.transactionId,
      };

      console.log(JSON.stringify(transactionCtx.meta, null, 4));

      await transaction.begin();

      transactionCtx.call(
        "dama_db.query",
        "CREATE TABLE spike_transaction_test AS SELECT 1 AS a ;"
      );

      try {
        const { rows: pCtxRows } = await ctx.call(
          "dama_db.query",
          "SELECT * FROM spike_transaction_test ;"
        );

        console.log(JSON.stringify({ pCtxRows }, null, 4));
      } catch (err) {
        console.log("If we see this error, ISOLATION works.");
        console.error(err);
      }

      const { rows: txnCtxRows } = await transactionCtx.call(
        "dama_db.query",
        "SELECT * FROM spike_transaction_test ;"
      );

      console.log(JSON.stringify({ txnCtxRows }, null, 4));

      await transaction.rollback();

      console.log(JSON.stringify(transaction.queryLog, null, 4));
    },
  },
};
