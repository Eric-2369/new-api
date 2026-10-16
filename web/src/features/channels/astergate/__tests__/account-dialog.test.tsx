import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, test, vi } from 'vitest'

import { api } from '@/lib/api'

import { AsterGateAccountDialog } from '../account-dialog'

function renderDialog() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
  render(
    <QueryClientProvider client={client}>
      <AsterGateAccountDialog
        channelId={12}
        channelName='My gateway'
        open
        onOpenChange={() => undefined}
      />
    </QueryClientProvider>
  )
}

afterEach(() => vi.restoreAllMocks())

describe('AsterGate account usage', () => {
  test('shows shared usage, preserves an unknown window and refreshes on demand', async () => {
    const get = vi.spyOn(api, 'get').mockResolvedValue({
      data: {
        success: true,
        data: {
          status: 'available',
          five_hour: { used_percent: 25.5 },
          weekly: null,
        },
      },
    })
    renderDialog()
    expect(await screen.findByText('25.5%')).toBeInTheDocument()
    expect(screen.getByText('—')).toBeInTheDocument()
    expect(
      screen.getByText(
        'Shared upstream account usage, not usage of this key alone'
      )
    ).toBeInTheDocument()
    await userEvent.click(screen.getByRole('button', { name: 'Refresh' }))
    await waitFor(() => expect(get).toHaveBeenCalledTimes(2))
    expect(get).toHaveBeenCalledWith(
      '/api/channel/12/astergate/usage',
      expect.objectContaining({ signal: expect.any(AbortSignal) })
    )
  })

  test('explains unavailable usage without claiming zero consumption', async () => {
    vi.spyOn(api, 'get').mockResolvedValue({
      data: {
        success: true,
        data: {
          status: 'unavailable',
          reason: 'key_not_bound_to_single_account',
          five_hour: null,
          weekly: null,
        },
      },
    })
    renderDialog()
    expect(await screen.findByRole('status')).toHaveTextContent(
      'Bind this upstream key to one direct Claude account to share usage'
    )
    expect(screen.queryByText('0%')).not.toBeInTheDocument()
  })

  test('distinguishes account authorization from missing observations', async () => {
    vi.spyOn(api, 'get').mockResolvedValue({
      data: {
        success: true,
        data: { status: 'unavailable', reason: 'account_not_authorized' },
      },
    })
    renderDialog()
    expect(await screen.findByRole('status')).toHaveTextContent(
      'The bound upstream account is unavailable or not authorized for this key'
    )
    expect(
      screen.queryByText('No recent upstream usage observation is available')
    ).not.toBeInTheDocument()
  })

  test('shows loading, then a retryable error when the request fails', async () => {
    let rejectRequest: (error: Error) => void = () => undefined
    vi.spyOn(api, 'get').mockImplementation(
      () =>
        new Promise((_resolve, reject) => {
          rejectRequest = reject
        })
    )
    renderDialog()
    expect(screen.getByText('Loading...')).toBeInTheDocument()
    rejectRequest(new Error('network failure'))
    expect(await screen.findByText('Failed to fetch usage')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled()
  })
})
