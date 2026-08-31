{{- define "agentmesh.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "agentmesh.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else if contains (include "agentmesh.name" .) .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "agentmesh.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "agentmesh.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "agentmesh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "agentmesh.selectorLabels" -}}
app.kubernetes.io/name: {{ include "agentmesh.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "agentmesh.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "agentmesh.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "agentmesh.secretName" -}}
{{- default (printf "%s-runtime" (include "agentmesh.fullname" .)) .Values.secrets.existingSecret }}
{{- end }}

{{- define "agentmesh.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "agentmesh.dashboardImage" -}}
{{- if .Values.dashboard.image.digest -}}
{{- printf "%s@%s" .Values.dashboard.image.repository .Values.dashboard.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.dashboard.image.repository (.Values.dashboard.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
{{- end }}

{{- define "agentmesh.runtimeEnv" -}}
- name: AGENTMESH_MODE
  value: distributed
- name: AGENTMESH_INSTANCE_ID
  valueFrom:
    fieldRef:
      fieldPath: metadata.name
- name: AGENTMESH_WORKERS
  value: {{ .Values.config.workers | quote }}
- name: AGENTMESH_ATTEMPT_TIMEOUT
  value: {{ .Values.config.attemptTimeout | quote }}
- name: AGENTMESH_SHUTDOWN_TIMEOUT
  value: {{ .Values.config.shutdownTimeout | quote }}
- name: AGENTMESH_MAX_ATTEMPTS
  value: {{ .Values.config.maxAttempts | quote }}
- name: AGENTMESH_RETRY_INITIAL_BACKOFF
  value: {{ .Values.config.retryInitialBackoff | quote }}
- name: AGENTMESH_RETRY_MAX_BACKOFF
  value: {{ .Values.config.retryMaxBackoff | quote }}
- name: AGENTMESH_NATS_ACK_WAIT
  value: {{ .Values.config.natsAckWait | quote }}
- name: AGENTMESH_CACHE_TTL
  value: {{ .Values.config.cacheTTL | quote }}
- name: AGENTMESH_LEASE_TTL
  value: {{ .Values.config.leaseTTL | quote }}
- name: AGENTMESH_EVENT_RETENTION
  value: {{ .Values.config.eventRetention | quote }}
- name: AGENTMESH_EVENT_HISTORY_LIMIT
  value: {{ .Values.config.eventHistoryLimit | quote }}
- name: AGENTMESH_WORKFLOW_CONCURRENCY
  value: {{ .Values.config.workflowConcurrency | quote }}
- name: AGENTMESH_WORKFLOW_LEASE_TTL
  value: {{ .Values.config.workflowLeaseTTL | quote }}
- name: AGENTMESH_APPROVAL_TTL
  value: {{ .Values.config.approvalTTL | quote }}
- name: AGENTMESH_APPROVAL_RETENTION
  value: {{ .Values.config.approvalRetention | quote }}
- name: AGENTMESH_MCP_SERVERS
  valueFrom:
    configMapKeyRef:
      name: {{ include "agentmesh.fullname" . }}
      key: mcp-servers
- name: AGENTMESH_DATABASE_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "agentmesh.secretName" . }}
      key: {{ .Values.secrets.databaseURLKey }}
- name: AGENTMESH_NATS_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "agentmesh.secretName" . }}
      key: {{ .Values.secrets.natsURLKey }}
- name: AGENTMESH_REDIS_URL
  valueFrom:
    secretKeyRef:
      name: {{ include "agentmesh.secretName" . }}
      key: {{ .Values.secrets.redisURLKey }}
- name: AGENTMESH_API_AUTH_CONFIG
  valueFrom:
    secretKeyRef:
      name: {{ include "agentmesh.secretName" . }}
      key: {{ .Values.secrets.apiAuthConfigKey }}
      optional: true
- name: AGENTMESH_AGENT_AUTH_CONFIG
  valueFrom:
    secretKeyRef:
      name: {{ include "agentmesh.secretName" . }}
      key: {{ .Values.secrets.agentAuthConfigKey }}
      optional: true
{{ with .Values.config.extraEnv }}
{{ toYaml . }}
{{ end }}
{{- end }}
