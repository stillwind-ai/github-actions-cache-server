{{/*
Expand the name of the chart.
*/}}
{{- define "github-actions-cache-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "github-actions-cache-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "github-actions-cache-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "github-actions-cache-server.labels" -}}
helm.sh/chart: {{ include "github-actions-cache-server.chart" . }}
{{ include "github-actions-cache-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "github-actions-cache-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "github-actions-cache-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "github-actions-cache-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "github-actions-cache-server.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
PVC name
*/}}
{{- define "github-actions-cache-server.pvcName" -}}
{{ include "github-actions-cache-server.fullname" . }}-data
{{- end }}

{{/*
Determine if PVC should be enabled.
If persistentVolumeClaim.enabled is explicitly set (true/false), use that value.
Otherwise enabled: cache files live on the volume.
*/}}
{{- define "github-actions-cache-server.pvcEnabled" -}}
{{- if kindIs "bool" .Values.persistentVolumeClaim.enabled -}}
  {{- .Values.persistentVolumeClaim.enabled -}}
{{- else -}}
  true
{{- end -}}
{{- end }}

{{/*
Check if multiple replicas are possible (autoscaling enabled or replicaCount > 1).
*/}}
{{- define "github-actions-cache-server.multipleReplicas" -}}
{{- if or (and .Values.autoscaling.enabled (gt (.Values.autoscaling.maxReplicas | int) 1)) (gt (.Values.replicaCount | int) 1) -}}
true
{{- else -}}
false
{{- end -}}
{{- end }}

{{/*
Effective PVC access modes.
Automatically switches to ReadWriteMany when multiple replicas are possible,
to prevent errors when multiple pods attach to the same volume.
*/}}
{{- define "github-actions-cache-server.pvcAccessModes" -}}
{{- if eq (include "github-actions-cache-server.multipleReplicas" .) "true" -}}
- ReadWriteMany
{{- else -}}
{{ toYaml .Values.persistentVolumeClaim.accessModes }}
{{- end -}}
{{- end }}

{{/*
Validate configuration. Fails if incompatible settings are detected.
*/}}
{{- define "github-actions-cache-server.validate" -}}
{{- with .Values.config.db.postgres }}
{{- if not (or .url .host $.Values.existingSecret $.Values.extraEnvFrom $.Values.extraEnv) -}}
{{- fail "A PostgreSQL database is required: set config.db.postgres.url or config.db.postgres.host (or provide DB_POSTGRES_URL via existingSecret)." -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
Generate environment variables from config values.
*/}}
{{- define "github-actions-cache-server.env" -}}
- name: PORT
  value: "3000"
- name: API_BASE_URL
  value: {{ default (printf "http://%s.%s.svc.cluster.local:%v" (include "github-actions-cache-server.fullname" .) .Release.Namespace .Values.service.port) .Values.config.apiBaseUrl | quote }}
- name: EAGER_MERGE
  value: {{ .Values.config.eagerMerge | quote }}
- name: CACHE_CLEANUP_OLDER_THAN_DAYS
  value: {{ .Values.config.cacheCleanupOlderThanDays | quote }}
{{- if .Values.config.cacheMaxSizeBytes }}
- name: CACHE_MAX_SIZE_BYTES
  value: {{ .Values.config.cacheMaxSizeBytes | quote }}
{{- end }}
- name: CACHE_FILESYSTEM_MAX_USAGE_PERCENT
  value: {{ .Values.config.cacheFilesystemMaxUsagePercent | quote }}
- name: ORPHANED_STORAGE_GRACE_PERIOD_HOURS
  value: {{ .Values.config.orphanedStorageGracePeriodHours | quote }}
{{- if .Values.config.disableCleanupJobs }}
- name: DISABLE_CLEANUP_JOBS
  value: "true"
{{- end }}
{{- if .Values.config.debug }}
- name: DEBUG
  value: "true"
{{- end }}
{{- if .Values.config.managementApiKey }}
- name: MANAGEMENT_API_KEY
  value: {{ .Values.config.managementApiKey | quote }}
{{- end }}
- name: STORAGE_FILESYSTEM_PATH
  value: {{ .Values.config.storage.filesystem.path | quote }}
- name: STORAGE_FILESYSTEM_IO_URING
  value: {{ .Values.config.ioUring | quote }}
{{/* Database */}}
{{- with .Values.config.db.postgres }}
{{- if .maxConnections }}
- name: DB_POSTGRES_MAX_CONNECTIONS
  value: {{ .maxConnections | quote }}
{{- end }}
{{- if .url }}
- name: DB_POSTGRES_URL
  value: {{ .url | quote }}
{{- else }}
{{- if .database }}
- name: DB_POSTGRES_DATABASE
  value: {{ .database | quote }}
{{- end }}
{{- if .host }}
- name: DB_POSTGRES_HOST
  value: {{ .host | quote }}
{{- end }}
{{- if .port }}
- name: DB_POSTGRES_PORT
  value: {{ .port | quote }}
{{- end }}
{{- if .user }}
- name: DB_POSTGRES_USER
  value: {{ .user | quote }}
{{- end }}
{{- if .password }}
- name: DB_POSTGRES_PASSWORD
  value: {{ .password | quote }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}
