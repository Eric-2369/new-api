import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'

import { Dialog } from '@/components/dialog'
import { ErrorState } from '@/components/error-state'
import { LoadingState } from '@/components/loading-state'
import { Button } from '@/components/ui/button'
import { Progress } from '@/components/ui/progress'
import { toIntlLocale } from '@/i18n/languages'
import { api } from '@/lib/api'
import { formatNumber, formatDateTimeStr } from '@/lib/format'
import { createServerError } from '@/lib/server-error-message'

type UsageWindow = {
  used_percent: number
  resets_at?: string
  checked_at?: string
}

type UsageReport = {
  status: 'available' | 'unavailable'
  reason?: string
  five_hour: UsageWindow | null
  weekly: UsageWindow | null
}

type Props = {
  channelId: number
  channelName: string
  open: boolean
  onOpenChange: (open: boolean) => void
}

export function AsterGateAccountDialog(props: Props) {
  const { t, i18n } = useTranslation()
  const locale = toIntlLocale(i18n.resolvedLanguage || i18n.language)
  const query = useQuery({
    queryKey: ['channels', props.channelId, 'astergate-usage'],
    enabled: props.open,
    staleTime: 0,
    retry: false,
    meta: { errorToast: false },
    queryFn: async ({ signal }) => {
      const response = await api.get<{ success: boolean; data: UsageReport }>(
        `/api/channel/${props.channelId}/astergate/usage`,
        { signal }
      )
      if (!response.data.success) {
        throw createServerError(response.data, t('Failed to fetch usage'))
      }
      return response.data.data
    },
  })
  const reasons: Record<string, string> = {
    account_not_authorized: t(
      'The bound upstream account is unavailable or not authorized for this key'
    ),
    key_not_bound_to_single_account: t(
      'Bind this upstream key to one direct Claude account to share usage'
    ),
    account_not_direct: t(
      'Bind this upstream key to one direct Claude account to share usage'
    ),
    not_observed: t('No recent upstream usage observation is available'),
    usage_not_permitted: t('Usage sharing is disabled for this upstream key'),
  }

  return (
    <Dialog
      open={props.open}
      onOpenChange={props.onOpenChange}
      title={`AsterGate · ${t('Account Info')}`}
      description={props.channelName}
      footer={
        <Button
          variant='outline'
          disabled={query.isFetching}
          onClick={() => void query.refetch()}
        >
          {t('Refresh')}
        </Button>
      }
    >
      {query.isPending && <LoadingState />}
      {query.isError && (
        <ErrorState
          title={t('Failed to fetch usage')}
          onRetry={() => void query.refetch()}
        />
      )}
      {query.data && !query.isError && (
        <div className='space-y-4'>
          <p className='text-muted-foreground text-sm'>
            {t('Shared upstream account usage, not usage of this key alone')}
          </p>
          {query.data.status === 'unavailable' && (
            <p role='status'>
              {reasons[query.data.reason ?? ''] ??
                t('No recent upstream usage observation is available')}
            </p>
          )}
          {[
            { label: t('5-hour usage'), window: query.data.five_hour },
            { label: t('Weekly usage'), window: query.data.weekly },
          ].map(({ label, window }) => (
            <section
              key={label}
              aria-label={label}
              className='space-y-2 rounded-lg border p-3'
            >
              <div className='flex justify-between gap-2'>
                <span>{label}</span>
                <span>
                  {window
                    ? `${formatNumber(window.used_percent, locale)}%`
                    : '—'}
                </span>
              </div>
              {window && (
                <Progress value={window.used_percent} aria-label={label} />
              )}
              {window?.resets_at && (
                <p className='text-muted-foreground text-sm'>
                  {t('Reset time')}:{' '}
                  {formatDateTimeStr(new Date(window.resets_at))}
                </p>
              )}
              {window?.checked_at && (
                <p className='text-muted-foreground text-sm'>
                  {t('Updated At')}:{' '}
                  {formatDateTimeStr(new Date(window.checked_at))}
                </p>
              )}
            </section>
          ))}
        </div>
      )}
    </Dialog>
  )
}
