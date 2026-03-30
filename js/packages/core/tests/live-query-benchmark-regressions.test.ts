import { describe, expect, it } from 'vitest'

const rowShapeModule = import('../../../../scripts/cynos_benchmark_row_shape.mjs')

describe('live query benchmark project tracking regressions', () => {
  it('selects tracked project ids deterministically from the same row set regardless of row order', async () => {
    const { extractProjectIds } = await rowShapeModule
    const rows = [
      { issueId: 1001, projectId: 42 },
      { issueId: 1002, projectId: 7 },
      { issueId: 1003, projectId: 19 },
      { issueId: 1004, projectId: 42 },
      { issueId: 1005, projectId: 11 },
      { issueId: 1006, projectId: 7 },
    ]

    expect(extractProjectIds(rows, 3)).toEqual([7, 11, 19])
    expect(extractProjectIds([...rows].reverse(), 3)).toEqual([7, 11, 19])
  })

  it('selects tracked project ids deterministically from binary result-set views too', async () => {
    const { extractProjectIdsFromResultSet } = await rowShapeModule
    const projectIds = [42, 7, 19, 42, 11, 7]
    const reverseProjectIds = [...projectIds].reverse()

    const makeIssueResultSet = (ids: number[]) => ({
      length: ids.length,
      getNumber(rowIndex: number, columnIndex: number) {
        if (columnIndex !== 1) {
          throw new Error(`unexpected column index ${columnIndex}`)
        }
        return ids[rowIndex]
      },
    })

    expect(
      extractProjectIdsFromResultSet(
        'issue_stream_all',
        makeIssueResultSet(projectIds),
        3,
      ),
    ).toEqual([7, 11, 19])
    expect(
      extractProjectIdsFromResultSet(
        'issue_stream_all',
        makeIssueResultSet(reverseProjectIds),
        3,
      ),
    ).toEqual([7, 11, 19])
  })
})
