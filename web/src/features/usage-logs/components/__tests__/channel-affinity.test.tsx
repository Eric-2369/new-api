/*
Copyright (C) 2023-2026 QuantumNous

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program. If not, see <https://www.gnu.org/licenses/>.

For commercial licensing, please contact support@quantumnous.com
*/
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  flexRender,
  getCoreRowModel,
  useReactTable,
} from '@tanstack/react-table'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { expect, it } from 'vitest'

import { usageLogSchema } from '../../data/schema'
import type { ChannelAffinityInfo } from '../../types'
import { useCommonLogsColumns } from '../columns/common-logs-columns'
import { UsageLogsProvider } from '../usage-logs-provider'

function ChannelFixture(props: { affinity?: ChannelAffinityInfo }) {
  const columns = useCommonLogsColumns(true, false)
  const table = useReactTable({
    data: [
      usageLogSchema.parse({
        id: 1,
        user_id: 1,
        created_at: 1788840000,
        content: '',
        type: 2,
        channel: 7,
        channel_name: 'test channel',
        other: JSON.stringify({
          admin_info: { channel_affinity: props.affinity },
        }),
      }),
    ],
    columns,
    getCoreRowModel: getCoreRowModel(),
  })
  const cell = table
    .getRowModel()
    .rows[0].getAllCells()
    .find((cell) => cell.column.id === 'channel')
  if (!cell) throw new Error('Channel column is missing')
  return flexRender(cell.column.columnDef.cell, cell.getContext())
}

it.each([
  { affinity: undefined, reuse: false, rule: false },
  {
    affinity: { source: 'websocket_connection', selected_group: 'default' },
    reuse: true,
    rule: false,
  },
  {
    affinity: { rule_name: 'session', key_fp: 'fingerprint' },
    reuse: false,
    rule: true,
  },
])(
  'distinguishes connection reuse from rule cache statistics: $affinity',
  async ({ affinity, reuse, rule }) => {
    const user = userEvent.setup()
    render(
      <QueryClientProvider
        client={
          new QueryClient({ defaultOptions: { queries: { retry: false } } })
        }
      >
        <UsageLogsProvider>
          <ChannelFixture affinity={affinity} />
        </UsageLogsProvider>
      </QueryClientProvider>
    )
    expect(
      screen.queryByRole('img', { name: 'WebSocket connection reused' }) !==
        null
    ).toBe(reuse)
    expect(
      screen.queryByRole('button', { name: 'Channel Affinity' }) !== null
    ).toBe(rule)
    if (reuse) {
      await user.hover(
        screen.getByRole('img', { name: 'WebSocket connection reused' })
      )
      expect(
        await screen.findByText('WebSocket connection reused')
      ).toBeVisible()
      expect(screen.queryByText(/^Rule:/)).not.toBeInTheDocument()
    }
  }
)
