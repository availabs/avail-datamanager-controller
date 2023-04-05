import { PgClient } from "../../domain/PostgreSQLTypes";

export default abstract class AbstractTranscomEventsAggregateEtlSubprocessController {
  protected _etlStart: Date;
  protected etlEnd: Date;
  protected etlSubprocessIdx: number | null;

  constructor(
    protected readonly db: PgClient,
    protected etlControlId: number | null
  ) {}

  // SIDE EFFECT WARNING: etlStart initialized on first get
  //   (Facilitates extending this class.)
  protected get etlStart() {
    if (this._etlStart) {
      return this._etlStart;
    }

    this._etlStart = new Date();

    this.etlSubprocessIdx = null;
    return this._etlStart;
  }

  protected abstract get initialEtlSubProcessControlMetadata(): {
    etlTask: string;
    etlStart: Date;
    status: string;
  };

  protected async initializeSubprocessControlMetadataEntry() {
    if (!Number.isFinite(this.etlControlId)) {
      this.etlSubprocessIdx = -1;
    }

    if (Number.isFinite(this.etlSubprocessIdx)) {
      return;
    }

    const q = `
      UPDATE _transcom_admin.etl_control
        SET metadata =  jsonb_set(
                          metadata,
                          ARRAY[
                            'subprocesses',
                            jsonb_array_length(
                              metadata->'subprocesses'
                            )::TEXT
                          ],
                          '{}'::JSONB,
                          true
                        )
        WHERE ( id = ${this.etlControlId} )
      ;

      SELECT
          (
            jsonb_array_length(
              metadata->'subprocesses'
            ) - 1
          ) AS idx
        FROM _transcom_admin.etl_control
        WHERE ( id = ${this.etlControlId} )
      ;
    `;

    const result = await this.db.query(q);

    const {
      rows: [{ idx }],
      // @ts-ignore
    } = result[result.length - 1];

    // NO IDEA WHY THIS HAS NO EFFECT.
    this.etlSubprocessIdx = idx;

    await this.updateSubprocessControlMetadataEntry(
      [],
      this.initialEtlSubProcessControlMetadata,
      idx
    );

    return this.etlSubprocessIdx;
  }

  protected async finializeSubprocessControlMetadataEntry(
    etlSubprocessIdx: number
  ) {
    await this.updateSubprocessControlMetadataEntry(
      ["status"],
      "DONE",
      etlSubprocessIdx
    );
    await this.updateSubprocessControlMetadataEntry(
      ["etlEnd"],
      new Date(),
      etlSubprocessIdx
    );
  }

  // FIXME FIXME FIXME
  // Encountered a bug where setting this.etlSubprocessIdx in this.initialEtlSubProcessControlMetadata
  // has no effect. The value remains null. Probably has something to do with super/child class behavior.
  // The etlSubprocessIdx parameter is a hack to get everything working.
  protected async updateSubprocessControlMetadataEntry(
    path: string[],
    value: any,
    etlSubprocessIdx: number
  ) {
    this.etlSubprocessIdx = etlSubprocessIdx;

    if (!Number.isFinite(this.etlSubprocessIdx)) {
      await this.initializeSubprocessControlMetadataEntry();
    }

    if (this.etlSubprocessIdx === -1) {
      return;
    }

    const subprocessMetadataPath = [
      "subprocesses",
      `${this.etlSubprocessIdx}`,
      ...path,
    ];

    await this.db.query(
      `
        UPDATE _transcom_admin.etl_control
          SET metadata =  jsonb_set( metadata, $1, $2, true )
          WHERE ( id = $3 )
      `,
      [subprocessMetadataPath, JSON.stringify(value), this.etlControlId]
    );
  }

  abstract run(): void;
}
