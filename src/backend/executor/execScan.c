/*-------------------------------------------------------------------------
 *
 * execScan.c
 *	  This code provides support for generalized relation scans. ExecScan
 *	  is passed a node and a pointer to a function to "do the right thing"
 *	  and return a tuple from the relation. ExecScan then does the tedious
 *	  stuff - checking the qualification and projecting the tuple
 *	  appropriately.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/execScan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "miscadmin.h"
#include "utils/memutils.h"



/*
 * ExecScanFetch -- check interrupts & fetch next potential tuple
 *
 * This routine is concerned with substituting a test tuple if we are
 * inside an EvalPlanQual recheck.  If we aren't, just execute
 * the access method's next-tuple routine.
 */
static inline TupleTableSlot *
ExecScanFetch(ScanState *node,
			  ExecScanAccessMtd accessMtd,
			  ExecScanRecheckMtd recheckMtd)
{
	EState	   *estate = node->ps.state;

	CHECK_FOR_INTERRUPTS();

	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;

		/*
		 * We are inside an EvalPlanQual recheck.  Return the test tuple if
		 * one is available, after rechecking any access-method-specific
		 * conditions.
		 */
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid == 0)
		{
			/*
			 * This is a ForeignScan or CustomScan which has pushed down a
			 * join to the remote side.  The recheck method is responsible not
			 * only for rechecking the scan/join quals but also for storing
			 * the correct tuple in the slot.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			if (!(*recheckMtd) (node, slot))
				ExecClearTuple(slot);	/* would not be returned by scan */
			return slot;
		}
		else if (epqstate->relsubs_done[scanrelid - 1])
		{
			/*
			 * Return empty slot, as either there is no EPQ tuple for this rel
			 * or we already returned it.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			return ExecClearTuple(slot);
		}
		else if (epqstate->relsubs_slot[scanrelid - 1] != NULL)
		{
			/*
			 * Return replacement tuple provided by the EPQ caller.
			 */

			TupleTableSlot *slot = epqstate->relsubs_slot[scanrelid - 1];

			Assert(epqstate->relsubs_rowmark[scanrelid - 1] == NULL);

			/* Mark to remember that we shouldn't return it again */
			epqstate->relsubs_done[scanrelid - 1] = true;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
		else if (epqstate->relsubs_rowmark[scanrelid - 1] != NULL)
		{
			/*
			 * Fetch and return replacement tuple using a non-locking rowmark.
			 */

			TupleTableSlot *slot = node->ss_ScanTupleSlot;

			/* Mark to remember that we shouldn't return more */
			epqstate->relsubs_done[scanrelid - 1] = true;

			if (!EvalPlanQualFetchRowMark(epqstate, scanrelid, slot))
				return NULL;

			/* Return empty slot if we haven't got a test tuple */
			if (TupIsNull(slot))
				return NULL;

			/* Check if it meets the access-method conditions */
			if (!(*recheckMtd) (node, slot))
				return ExecClearTuple(slot);	/* would not be returned by
												 * scan */
			return slot;
		}
	}

	/*
	 * Run the node-type-specific access method function to get the next tuple
	 */
	return (*accessMtd) (node);
}

/* ----------------------------------------------------------------
 *		ExecScan
 *
 *		Scans the relation using the 'access method' indicated and
 *		returns the next qualifying tuple.
 *		The access method returns the next tuple and ExecScan() is
 *		responsible for checking the tuple returned against the qual-clause.
 *
 *		A 'recheck method' must also be provided that can check an
 *		arbitrary tuple of the relation against any qual conditions
 *		that are implemented internal to the access method.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 * ----------------------------------------------------------------
 */
#include <time.h>  // 引入用于计时的库
#include <stdio.h>  

// TupleTableSlot *ExecScan(ScanState *node,
//                          ExecScanAccessMtd accessMtd,
//                          ExecScanRecheckMtd recheckMtd)
// {
//     ExprContext *econtext;
//     ExprState *qual;
//     ProjectionInfo *projInfo;

//     long total_time = 0, qual_time = 0, proj_time = 0, exec_scanfetch_time = 0;

//     const char *sql_query = node->ps.state->es_sourceText;

//     FILE *fp = fopen("/Users/hzhong81/Documents/query_stats_for.txt", "a");

//     qual = node->ps.qual;
//     projInfo = node->ps.ps_ProjInfo;
//     econtext = node->ps.ps_ExprContext;

//     struct timespec start, end;

//     if (!qual && !projInfo) {
//         ResetExprContext(econtext);

//         TupleTableSlot *result = ExecScanFetch(node, accessMtd, recheckMtd);
//         return result;
//     }

//     ResetExprContext(econtext);

//     clock_gettime(CLOCK_MONOTONIC, &start);
//     for (;;) {

//         TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);
//         if (TupIsNull(slot)) {
//             if (projInfo) {
//                 TupleTableSlot *result = ExecClearTuple(projInfo->pi_state.resultslot);
//                 return result;
//             } else {
//                 return slot;
//             }
//         }

//         econtext->ecxt_scantuple = slot;
//         bool qual_result = (qual == NULL) || ExecQual(qual, econtext);

//         if (qual_result) {
//             if (projInfo) {
//                 TupleTableSlot *result = ExecProject(projInfo);

//                 if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return result;
//             } else {
//                 if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return slot;
//             }
//         } else {
// 			                if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//             InstrCountFiltered1(node, 1);
//         }

//         ResetExprContext(econtext);
//     }

//     return NULL;
// }

// TupleTableSlot *ExecScan(ScanState *node,
//                          ExecScanAccessMtd accessMtd,
//                          ExecScanRecheckMtd recheckMtd)
// {
//     ExprContext *econtext;
//     ExprState *qual;
//     ProjectionInfo *projInfo;

//     long total_time = 0, qual_time = 0, proj_time = 0, exec_scanfetch_time = 0;

//     const char *sql_query = node->ps.state->es_sourceText;

//     FILE *fp = fopen("/Users/hzhong81/Documents/query_stats_detailed.txt", "a");

//     qual = node->ps.qual;
//     projInfo = node->ps.ps_ProjInfo;
//     econtext = node->ps.ps_ExprContext;

//     struct timespec start, end;
//     clock_gettime(CLOCK_MONOTONIC, &start);

//     if (!qual && !projInfo) {
//         ResetExprContext(econtext);

//         struct timespec exec_fetch_start, exec_fetch_end;
//         clock_gettime(CLOCK_MONOTONIC, &exec_fetch_start);
//         TupleTableSlot *result = ExecScanFetch(node, accessMtd, recheckMtd);
//         clock_gettime(CLOCK_MONOTONIC, &exec_fetch_end);

//         exec_scanfetch_time = (exec_fetch_end.tv_sec - exec_fetch_start.tv_sec) * 1e9 +
//                               (exec_fetch_end.tv_nsec - exec_fetch_start.tv_nsec);

//         if (fp != NULL) {
//             fprintf(fp, "SQL Query: %s\n", sql_query);
//             fprintf(fp, "ExecScanFetch Time: %ld ns\n", exec_scanfetch_time);
//             fprintf(fp, "---------------------------------------\n");
//             fclose(fp);
//         }
//         return result;
//     }

//     ResetExprContext(econtext);

//     for (;;) {
//         struct timespec fetch_start, fetch_end, qual_start, qual_end, proj_start, proj_end;

//         clock_gettime(CLOCK_MONOTONIC, &fetch_start);
//         TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);
//         clock_gettime(CLOCK_MONOTONIC, &fetch_end);

//         exec_scanfetch_time += (fetch_end.tv_sec - fetch_start.tv_sec) * 1e9 +
//                                (fetch_end.tv_nsec - fetch_start.tv_nsec);

//         if (TupIsNull(slot)) {
//             if (projInfo) {
//                 TupleTableSlot *result = ExecClearTuple(projInfo->pi_state.resultslot);
                
//                 // Logging before return
//                 if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "ExecScanFetch Time: %ld ns\n", exec_scanfetch_time);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return result;
//             } else {
//                 if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "ExecScanFetch Time: %ld ns\n", exec_scanfetch_time);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return slot;
//             }
//         }

//         econtext->ecxt_scantuple = slot;

//         clock_gettime(CLOCK_MONOTONIC, &qual_start);
//         bool qual_result = (qual == NULL) || ExecQual(qual, econtext);
//         clock_gettime(CLOCK_MONOTONIC, &qual_end);

//         qual_time += (qual_end.tv_sec - qual_start.tv_sec) * 1e9 +
//                      (qual_end.tv_nsec - qual_start.tv_nsec);

//         if (qual_result) {
//             if (projInfo) {
//                 clock_gettime(CLOCK_MONOTONIC, &proj_start);
//                 TupleTableSlot *result = ExecProject(projInfo);
//                 clock_gettime(CLOCK_MONOTONIC, &proj_end);

//                 proj_time += (proj_end.tv_sec - proj_start.tv_sec) * 1e9 +
//                              (proj_end.tv_nsec - proj_start.tv_nsec);

//                 clock_gettime(CLOCK_MONOTONIC, &end);
//                 total_time = (end.tv_sec - start.tv_sec) * 1e9 +
//                              (end.tv_nsec - start.tv_nsec);

//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "ExecScanFetch Total Time: %ld ns\n", exec_scanfetch_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time, (double)qual_time / total_time * 100);
//                     fprintf(fp, "Projection Time: %ld ns (%.2f%%)\n", proj_time, (double)proj_time / total_time * 100);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return result;
//             } else {
//                 if (fp != NULL) {
//                     clock_gettime(CLOCK_MONOTONIC, &end);
//                     total_time = (end.tv_sec - start.tv_sec) * 1e9 + (end.tv_nsec - start.tv_nsec);
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
//                     fprintf(fp, "ExecScanFetch Time: %ld ns\n", exec_scanfetch_time);
//                     fprintf(fp, "Qual Time: %ld ns\n", qual_time);
//                     fprintf(fp, "Total Time: %ld ns\n", total_time);
//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return slot;
//             }
//         } else {
//             InstrCountFiltered1(node, 1);
//         }

//         ResetExprContext(econtext);
//     }

//     return NULL;
// }
// TupleTableSlot *ExecScan(ScanState *node,
//                          ExecScanAccessMtd accessMtd,
//                          ExecScanRecheckMtd recheckMtd)
// {
//     ExprContext *econtext;
//     ExprState *qual;
//     ProjectionInfo *projInfo;

//     long total_time = 0, qual_time = 0, proj_time = 0, skip_total_time = 0, fetch_total_time = 0,instr_total_time;

//     // 获取执行的 SQL 语句信息
//     const char *sql_query = node->ps.state->es_sourceText;
//     FILE *fp = fopen("/Users/hzhong81/Documents/query_stats_execscan.txt", "a");

//     // 初始化 node 中的数据
//     qual = node->ps.qual;
//     projInfo = node->ps.ps_ProjInfo;
//     econtext = node->ps.ps_ExprContext;

//     if (!qual && !projInfo) {
//         ResetExprContext(econtext);
//         // 执行 ExecScanFetch
// 				if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "This is ExecScanFetch\n");
// 					                    fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
// 				}
//         TupleTableSlot *result = ExecScanFetch(node, accessMtd, recheckMtd);
//         return result;
//         // return ExecScanFetch(node, accessMtd, recheckMtd);
//     }

//     ResetExprContext(econtext);

//     struct timespec skip_start, skip_end;
//     clock_gettime(CLOCK_MONOTONIC, &skip_start);

//     for (;;) {
//         struct timespec start, end;
//         clock_gettime(CLOCK_MONOTONIC, &start);

//         TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);

//         clock_gettime(CLOCK_MONOTONIC, &end);
//         fetch_total_time = (end.tv_sec - start.tv_sec) * 1e9 +
//                              (end.tv_nsec - start.tv_nsec);

//         if (TupIsNull(slot)) {
//             if (projInfo) {
// 				if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "projinfo + tupIsNull\n");
// 					                    fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
// 				}
//                 return ExecClearTuple(projInfo->pi_state.resultslot);
//             } else {
// 				if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "tupIsNull\n");
// 					                    fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
// 				}
//                 return slot;
//             }
//         }

//         econtext->ecxt_scantuple = slot;

//         // 记录 qual 执行时间
//         struct timespec qual_start, qual_end;
//         clock_gettime(CLOCK_MONOTONIC, &qual_start);

//         bool qual_result = (qual == NULL) || ExecQual(qual, econtext);

//         clock_gettime(CLOCK_MONOTONIC, &qual_end);
//         qual_time = (qual_end.tv_sec - qual_start.tv_sec) * 1e9 +
//                     (qual_end.tv_nsec - qual_start.tv_nsec);

//         if (qual_result) {
//             // 如果符合过滤条件，执行 projInfo
//             if (projInfo) {
//                 struct timespec proj_start, proj_end;
//                 clock_gettime(CLOCK_MONOTONIC, &proj_start);

//                 TupleTableSlot *result = ExecProject(projInfo);

//                 clock_gettime(CLOCK_MONOTONIC, &proj_end);
//                 proj_time = (proj_end.tv_sec - proj_start.tv_sec) * 1e9 +
//                             (proj_end.tv_nsec - proj_start.tv_nsec);

//                 clock_gettime(CLOCK_MONOTONIC, &end);
//                 clock_gettime(CLOCK_MONOTONIC, &skip_end);

//                 total_time = (end.tv_sec - start.tv_sec) * 1e9 +
//                              (end.tv_nsec - start.tv_nsec);
//                 skip_total_time = (skip_end.tv_sec - skip_start.tv_sec) * 1e9 +
//                                   (skip_end.tv_nsec - skip_start.tv_nsec);

//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "qualresult\n");
//                     fprintf(fp, "Skip until this row SeqScan Time: %ld ns\n", skip_total_time);
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", total_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time,
//                             (double)qual_time / total_time * 100);
//                     fprintf(fp, "Projection Time: %ld ns (%.2f%%)\n", proj_time,
//                             (double)proj_time / total_time * 100);
//                 //     // 输出当前行的列值
//                     for (int i = 0; i < slot->tts_nvalid; i++) {
//                         bool isnull = slot->tts_isnull[i];
//                         Datum value = slot->tts_values[i];
//                         if (isnull) {
//                             fprintf(fp, "Column %d: NULL\n", i + 1);
//                         } else {
//                             fprintf(fp, "Column %d: %ld\n", i + 1, value);
//                         }
//                     }

//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
//                 }
//                 return result;
//             } else {
// 				clock_gettime(CLOCK_MONOTONIC, &skip_end);

//                 skip_total_time = (skip_end.tv_sec - skip_start.tv_sec) * 1e9 +
//                                   (skip_end.tv_nsec - skip_start.tv_nsec);
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "return slot\n");
//                     fprintf(fp, "Skip until this row SeqScan Time: %ld ns\n", skip_total_time);
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", total_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", 0,
//                             (double)qual_time / total_time * 100);
//                     fprintf(fp, "Projection Time: %ld ns (%.2f%%)\n", proj_time,
//                             (double)proj_time / total_time * 100);

//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
// 				}
//                 return slot;
//             }
//         } else {
// 			        struct timespec instr_start, instr_end;
//         clock_gettime(CLOCK_MONOTONIC, &instr_start);

//             InstrCountFiltered1(node, 1);
//         clock_gettime(CLOCK_MONOTONIC, &instr_end);
// 		instr_total_time = (instr_end.tv_sec - instr_start.tv_sec) * 1e9 +
//                                   (instr_end.tv_nsec - instr_start.tv_nsec);
// 				clock_gettime(CLOCK_MONOTONIC, &skip_end);

//                 skip_total_time = (skip_end.tv_sec - skip_start.tv_sec) * 1e9 +
//                                   (skip_end.tv_nsec - skip_start.tv_nsec);
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "InstrCountFiltered1\n");
//                     fprintf(fp, "Skip until this row SeqScan Time: %ld ns\n", skip_total_time);
// 					fprintf(fp, "instr count time: %ld ns\n", instr_total_time);
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", fetch_total_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time,
//                             (double)qual_time / total_time * 100);
//                     fprintf(fp, "Projection Time: %ld ns (%.2f%%)\n", proj_time,
//                             (double)proj_time / total_time * 100);

//                     fprintf(fp, "---------------------------------------\n");
//                     fclose(fp);
// 				}
//         }


//         ResetExprContext(econtext);
//     }


//     return NULL;
// }

// TupleTableSlot *ExecScan(ScanState *node,
//                          ExecScanAccessMtd accessMtd,
//                          ExecScanRecheckMtd recheckMtd)
// {
//     ExprContext *econtext;
//     ExprState *qual;
//     ProjectionInfo *projInfo;

//     long total_time = 0, qual_time = 0, proj_time = 0, skip_total_time = 0, fetch_total_time = 0,scan_time = 0,gap_time=0;
//     struct timespec start, end;
//     // 获取执行的 SQL 语句信息
//     const char *sql_query = node->ps.state->es_sourceText;
//     FILE *fp = fopen("/Users/hzhong81/Documents/query_stats_execscan.txt", "a");
//     // 初始化 node 中的数据
//     qual = node->ps.qual;
//     projInfo = node->ps.ps_ProjInfo;
//     econtext = node->ps.ps_ExprContext;
//     if (!qual && !projInfo) {
//         ResetExprContext(econtext);
//         TupleTableSlot *result = ExecScanFetch(node, accessMtd, recheckMtd);
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fclose(fp);
//                 }
//         return result;
//         // return ExecScanFetch(node, accessMtd, recheckMtd);
//     }
//     clock_gettime(CLOCK_MONOTONIC, &start);
//     ResetExprContext(econtext);
//     for (;;) {
//         struct timespec scan_start, scan_end;
//         clock_gettime(CLOCK_MONOTONIC, &scan_start);
//         TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);
//         clock_gettime(CLOCK_MONOTONIC, &scan_end);
//         scan_time = (scan_end.tv_sec - scan_start.tv_sec) * 1e9 +
//                     (scan_end.tv_nsec - scan_start.tv_nsec);

//                 // // // 将统计结果写入文件
//                 // if (fp != NULL) {
//                 //     fprintf(fp, "SQL Query: %s\n", sql_query);
// 				// 	fprintf(fp, "Fetchresult\n");
// 				// 	fprintf(fp, "fetch time: %ld ns\n", scan_time);		
//                 //     fprintf(fp, "---------------------------------------\n");
// 				// 	fflush(fp);
//                 //     fclose(fp);
// 				// 	}

//         if (TupIsNull(slot)) {
//             if (projInfo) {
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fclose(fp);
//                 }
//                 return ExecClearTuple(projInfo->pi_state.resultslot);
//             } else {
//                 // // 将统计结果写入文件
//                  if (fp != NULL) {
//                     fclose(fp);
//                 }
//                 return slot;
//             }
//         }

//         econtext->ecxt_scantuple = slot;

//         // 记录 qual 执行时间
//         struct timespec qual_start, qual_end;
//         clock_gettime(CLOCK_MONOTONIC, &qual_start);

//         bool qual_result = (qual == NULL) || ExecQual(qual, econtext);

//         clock_gettime(CLOCK_MONOTONIC, &qual_end);
//         qual_time = (qual_end.tv_sec - qual_start.tv_sec) * 1e9 +
//                     (qual_end.tv_nsec - qual_start.tv_nsec);

//         if (qual_result) {
//             // 如果符合过滤条件，执行 projInfo
//             if (projInfo) {
// 				struct timespec proj_start, proj_end;
// 				clock_gettime(CLOCK_MONOTONIC, &proj_start);
//                 TupleTableSlot *result = ExecProject(projInfo);
// 				clock_gettime(CLOCK_MONOTONIC, &proj_end);
//        			clock_gettime(CLOCK_MONOTONIC, &end);
// 				proj_time = (proj_end.tv_sec - proj_start.tv_sec) * 1e9 +
//                              (proj_end.tv_nsec - proj_start.tv_nsec);
//                 total_time = (end.tv_sec - start.tv_sec) * 1e9 +
//                              (end.tv_nsec - start.tv_nsec);
// 				total_time -= gap_time;
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "qualresult\n");
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", total_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time,
//                             (double)qual_time / total_time * 100);
// 					fprintf(fp, "proj time: %ld ns\n", proj_time);		
// 					fprintf(fp, "fetch time: %ld ns\n", scan_time);		
// 				                //     // 输出当前行的列值
//                     for (int i = 0; i < slot->tts_nvalid; i++) {
//                         bool isnull = slot->tts_isnull[i];
//                         Datum value = slot->tts_values[i];
//                         if (isnull) {
//                             fprintf(fp, "Column %d: NULL\n", i + 1);
//                         } else {
//                             fprintf(fp, "Column %d: %ld\n", i + 1, value);
//                         }
//                     }
//                     fprintf(fp, "---------------------------------------\n");
// 					fflush(fp);
// 					fclose(fp);
//                 }
//                 return result;
//             } else {
//        			clock_gettime(CLOCK_MONOTONIC, &end);
//                 total_time = (end.tv_sec - start.tv_sec) * 1e9 +
//                              (end.tv_nsec - start.tv_nsec);
// 				total_time -= gap_time;
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "normalresult\n");
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", total_time);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time,
//                             (double)qual_time / total_time * 100);
// 					fprintf(fp, "proj time: %ld ns\n", proj_time);		
// 					fprintf(fp, "fetch time: %ld ns\n", scan_time);		
//                     fprintf(fp, "---------------------------------------\n");
// 					fflush(fp);
//                     fclose(fp);
//                 }
//                 return slot;
//             }
//         } else {
//             InstrCountFiltered1(node, 1);
// 			struct timespec gap_start, gap_end;
//         	clock_gettime(CLOCK_MONOTONIC, &gap_start);
//                 // // 将统计结果写入文件
//                 if (fp != NULL) {
//                     fprintf(fp, "SQL Query: %s\n", sql_query);
// 					fprintf(fp, "normalresult\n");
//                     fprintf(fp, "This row SeqScan Time: %ld ns\n", 0);
//                     fprintf(fp, "Qual Time: %ld ns (%.2f%%)\n", qual_time,
//                             (double)qual_time / total_time * 100);
// 					fprintf(fp, "proj time: %ld ns\n", 0);		
// 					fprintf(fp, "fetch time: %ld ns\n", scan_time);		
//                     fprintf(fp, "---------------------------------------\n");
// 					fflush(fp);
//                 }
//         	clock_gettime(CLOCK_MONOTONIC, &gap_end);
//             gap_time += (gap_end.tv_sec - gap_start.tv_sec) * 1e9 +
//                              (gap_end.tv_nsec - gap_start.tv_nsec);
//         }


//         ResetExprContext(econtext);
//     }

//                 if (fp != NULL) {
//                     fclose(fp);
//                 }
//     return NULL;
// }

TupleTableSlot *ExecScan(ScanState *node,
                         ExecScanAccessMtd accessMtd,
                         ExecScanRecheckMtd recheckMtd)
{
    ExprContext *econtext;
    ExprState *qual;
    ProjectionInfo *projInfo;

    qual = node->ps.qual;
    projInfo = node->ps.ps_ProjInfo;
    econtext = node->ps.ps_ExprContext;

    if (!qual && !projInfo) {
        ResetExprContext(econtext);
        return ExecScanFetch(node, accessMtd, recheckMtd);
    }

    ResetExprContext(econtext);


    for (;;) {
        TupleTableSlot *slot = ExecScanFetch(node, accessMtd, recheckMtd);

        if (TupIsNull(slot)) {
            if (projInfo) {
                return ExecClearTuple(projInfo->pi_state.resultslot);
            } else {
                return slot;
            }
        }

        econtext->ecxt_scantuple = slot;
        bool qual_result = (qual == NULL) || ExecQual(qual, econtext);

        if (qual_result) {
            if (projInfo) {
                TupleTableSlot *result = ExecProject(projInfo);
                return result;
            } else {
                return slot;
            }
        } else {
            InstrCountFiltered1(node, 1);
        }


        ResetExprContext(econtext);
    }

    return NULL;
}


/*
 * ExecAssignScanProjectionInfo
 *		Set up projection info for a scan node, if necessary.
 *
 * We can avoid a projection step if the requested tlist exactly matches
 * the underlying tuple type.  If so, we just set ps_ProjInfo to NULL.
 * Note that this case occurs not only for simple "SELECT * FROM ...", but
 * also in most cases where there are joins or other processing nodes above
 * the scan node, because the planner will preferentially generate a matching
 * tlist.
 *
 * The scan slot's descriptor must have been set already.
 */
void
ExecAssignScanProjectionInfo(ScanState *node)
{
	Scan	   *scan = (Scan *) node->ps.plan;
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, scan->scanrelid);
}

/*
 * ExecAssignScanProjectionInfoWithVarno
 *		As above, but caller can specify varno expected in Vars in the tlist.
 */
void
ExecAssignScanProjectionInfoWithVarno(ScanState *node, Index varno)
{
	TupleDesc	tupdesc = node->ss_ScanTupleSlot->tts_tupleDescriptor;

	ExecConditionalAssignProjectionInfo(&node->ps, tupdesc, varno);
}

/*
 * ExecScanReScan
 *
 * This must be called within the ReScan function of any plan node type
 * that uses ExecScan().
 */
void
ExecScanReScan(ScanState *node)
{
	EState	   *estate = node->ps.state;

	/*
	 * We must clear the scan tuple so that observers (e.g., execCurrent.c)
	 * can tell that this plan node is not positioned on a tuple.
	 */
	ExecClearTuple(node->ss_ScanTupleSlot);

	/*
	 * Rescan EvalPlanQual tuple(s) if we're inside an EvalPlanQual recheck.
	 * But don't lose the "blocked" status of blocked target relations.
	 */
	if (estate->es_epq_active != NULL)
	{
		EPQState   *epqstate = estate->es_epq_active;
		Index		scanrelid = ((Scan *) node->ps.plan)->scanrelid;

		if (scanrelid > 0)
			epqstate->relsubs_done[scanrelid - 1] =
				epqstate->epqExtra->relsubs_blocked[scanrelid - 1];
		else
		{
			Bitmapset  *relids;
			int			rtindex = -1;

			/*
			 * If an FDW or custom scan provider has replaced the join with a
			 * scan, there are multiple RTIs; reset the epqScanDone flag for
			 * all of them.
			 */
			if (IsA(node->ps.plan, ForeignScan))
				relids = ((ForeignScan *) node->ps.plan)->fs_relids;
			else if (IsA(node->ps.plan, CustomScan))
				relids = ((CustomScan *) node->ps.plan)->custom_relids;
			else
				elog(ERROR, "unexpected scan node: %d",
					 (int) nodeTag(node->ps.plan));

			while ((rtindex = bms_next_member(relids, rtindex)) >= 0)
			{
				Assert(rtindex > 0);
				epqstate->relsubs_done[rtindex - 1] =
					epqstate->epqExtra->relsubs_blocked[rtindex - 1];
			}
		}
	}
}
