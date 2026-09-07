import type {
  ApiErrorEnvelope,
  ApprovalList,
  Campaign,
  CampaignComparisonResponse,
  CampaignDocumentResponse,
  EventPage,
  ReportResponse,
  Run,
  RunCreateInput,
} from './domain';

export class ControlPlaneApiError extends Error {
  public readonly status: number;
  public readonly code: string;

  public constructor(status: number, code: string, message: string) {
    super(message);
    this.name = 'ControlPlaneApiError';
    this.status = status;
    this.code = code;
  }
}

export interface PersistedEvents {
  events: EventPage['events'];
  cursor: string | null;
}

export class ControlPlaneClient {
  private readonly baseUrl: string;
  private readonly fetcher: typeof fetch;

  public constructor(baseUrl = configuredBaseUrl(), fetcher = fetch) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.fetcher = fetcher;
  }

  public apiUrl(path: string): string {
    return `${this.baseUrl}/api/v1${path}`;
  }

  public getRun(runId: string): Promise<Run> {
    return this.request<Run>(`/runs/${encodeURIComponent(runId)}`);
  }

  public createRun(input: RunCreateInput): Promise<Run> {
    return this.request<Run>('/runs', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(input),
    });
  }

  public cancelRun(
    runId: string,
  ): Promise<{ run: Run; already_cancelled: boolean }> {
    return this.request(`/runs/${encodeURIComponent(runId)}/cancel`, {
      method: 'POST',
    });
  }

  public async getAllEvents(
    runId: string,
    pageSize = 200,
    initialCursor: string | null = null,
  ): Promise<PersistedEvents> {
    const events: EventPage['events'] = [];
    let cursor = initialCursor;
    let hasMore = true;
    while (hasMore) {
      const params = new URLSearchParams({ limit: String(pageSize) });
      if (cursor !== null) params.set('cursor', cursor);
      const page = await this.request<EventPage>(
        `/runs/${encodeURIComponent(runId)}/events?${params.toString()}`,
      );
      events.push(...page.events);
      hasMore = page.has_more;
      if (page.next_cursor === null) {
        if (hasMore)
          throw new ControlPlaneApiError(
            500,
            'invalid_event_page',
            'Event replay stopped unexpectedly.',
          );
        break;
      }
      if (page.next_cursor === cursor && hasMore) {
        throw new ControlPlaneApiError(
          500,
          'invalid_event_page',
          'Event replay did not advance.',
        );
      }
      cursor = page.next_cursor;
    }
    return { events, cursor };
  }

  public async getReport(runId: string): Promise<ReportResponse | null> {
    try {
      return await this.request<ReportResponse>(
        `/runs/${encodeURIComponent(runId)}/report`,
      );
    } catch (error) {
      if (
        error instanceof ControlPlaneApiError &&
        (error.status === 404 || error.status === 409)
      ) {
        return null;
      }
      throw error;
    }
  }

  public getApprovals(runId: string): Promise<ApprovalList> {
    return this.request(`/runs/${encodeURIComponent(runId)}/approvals`);
  }

  public resolveApproval(
    approvalId: string,
    result: 'approved' | 'denied',
    actorId: string,
  ): Promise<unknown> {
    return this.request(
      `/approvals/${encodeURIComponent(approvalId)}/resolve`,
      {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ result, actor_id: actorId }),
      },
    );
  }

  public getCampaign(campaignId: string): Promise<Campaign> {
    return this.request(`/campaigns/${encodeURIComponent(campaignId)}`);
  }

  public getCampaignStatistics(
    campaignId: string,
  ): Promise<CampaignDocumentResponse> {
    return this.request(
      `/campaigns/${encodeURIComponent(campaignId)}/statistics?k=1`,
    );
  }

  public compareCampaigns(
    baselineCampaignId: string,
    faultedCampaignId: string,
  ): Promise<CampaignComparisonResponse> {
    return this.request(
      `/campaigns/${encodeURIComponent(baselineCampaignId)}/compare/${encodeURIComponent(faultedCampaignId)}?k=1`,
    );
  }

  private async request<T>(path: string, init?: RequestInit): Promise<T> {
    let response: Response;
    try {
      const headers = new Headers(init?.headers);
      headers.set('accept', 'application/json');
      response = await this.fetcher(this.apiUrl(path), {
        ...init,
        headers,
      });
    } catch {
      throw new ControlPlaneApiError(
        0,
        'api_unavailable',
        'The control plane is unavailable.',
      );
    }
    if (!response.ok) throw await apiError(response);
    try {
      return (await response.json()) as T;
    } catch {
      throw new ControlPlaneApiError(
        502,
        'invalid_api_response',
        'The control plane returned an invalid response.',
      );
    }
  }
}

function configuredBaseUrl(): string {
  const value: unknown = import.meta.env.VITE_CONTROL_PLANE_URL;
  return typeof value === 'string' ? value : '';
}

async function apiError(response: Response): Promise<ControlPlaneApiError> {
  try {
    const value = (await response.json()) as Partial<ApiErrorEnvelope>;
    const code = value.error?.code;
    const message = value.error?.message;
    if (typeof code === 'string' && typeof message === 'string') {
      return new ControlPlaneApiError(response.status, code, message);
    }
  } catch {
    // Fall through to the bounded transport error below.
  }
  return new ControlPlaneApiError(
    response.status,
    'request_failed',
    'The control-plane request could not be completed.',
  );
}
