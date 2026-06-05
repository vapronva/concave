{{- define "convex.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "convex.labelPrefix" -}}
{{- .Values.labelPrefix | default "convex" -}}
{{- end -}}

{{- define "convex.selectorLabels" -}}
app.kubernetes.io/name: {{ include "convex.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "convex.labels" -}}
helm.sh/chart: {{ include "convex.chart" . }}
{{ include "convex.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: convex
{{- end -}}

{{- define "convex.componentLabels" -}}
{{- $ctx := .ctx -}}
{{ include "convex.labels" $ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "convex.componentSelectorLabels" -}}
{{- $ctx := .ctx -}}
{{ include "convex.selectorLabels" $ctx }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "convex.leaderName" -}}{{ include "convex.fullname" . }}-leader{{- end -}}
{{- define "convex.followerName" -}}{{ include "convex.fullname" . }}-follower{{- end -}}
{{- define "convex.funrunName" -}}{{ include "convex.fullname" . }}-funrun{{- end -}}
{{- define "convex.dashboardName" -}}{{ include "convex.fullname" . }}-dashboard{{- end -}}
{{- define "convex.envConfigName" -}}{{ include "convex.fullname" . }}-env{{- end -}}
{{- define "convex.ckicName" -}}{{ include "convex.fullname" . }}-ckic{{- end -}}

{{- define "convex.instanceSecretName" -}}
{{- default (printf "%s-instance" (include "convex.fullname" .)) .Values.instance.secretRef.name -}}
{{- end -}}

{{- define "convex.dbSecretName" -}}
{{- default (printf "%s-db" (include "convex.fullname" .)) .Values.db.passwordRef.name -}}
{{- end -}}

{{- define "convex.dashboardSecretName" -}}
{{- default (printf "%s-dashboard" (include "convex.fullname" .)) .Values.dashboard.adminKeyRef.name -}}
{{- end -}}

{{- define "convex.image" -}}
{{- $ctx := .ctx -}}
{{- $top := $ctx.Values.image -}}
{{- $spec := .spec -}}
{{- $registry := default $top.registry $spec.registry -}}
{{- $repo := default $top.repository $spec.repository -}}
{{- $tag := $spec.tag | default $top.tag | default $ctx.Chart.AppVersion -}}
{{- $ref := $repo -}}
{{- if $registry -}}
{{- $ref = printf "%s/%s" $registry $repo -}}
{{- end -}}
{{- printf "%s:%s" $ref $tag -}}
{{- end -}}

{{- define "convex.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 0 }}
{{- end }}
{{- end -}}

{{- define "convex.backendDiscoveryLabels" -}}
{{- $ctx := .ctx -}}
{{- $prefix := include "convex.labelPrefix" $ctx -}}
{{ $prefix }}/component: backend
{{ $prefix }}/instance: {{ required "instance.name is required" $ctx.Values.instance.name | quote }}
{{ $prefix }}/role: {{ required "role is required for backend discovery labels" .role }}
{{ $prefix }}/leader-priority: {{ .priority | quote }}
{{- end -}}

{{- define "convex.componentDiscoveryLabel" -}}
{{- $prefix := include "convex.labelPrefix" .ctx -}}
{{ $prefix }}/component: {{ .component }}
{{- end -}}

{{- define "convex.storageMountPath" -}}
{{- default "/convex/data" .Values.storage.nfs.mountPath -}}
{{- end -}}

{{- define "convex.storageDir" -}}
{{- printf "%s/storage" (include "convex.storageMountPath" .) -}}
{{- end -}}

{{- define "convex.dataVolume" -}}
{{- $nfs := .Values.storage.nfs -}}
- name: data
  nfs:
    server: {{ required "storage.nfs.server is required" $nfs.server | quote }}
    path: {{ required "storage.nfs.path is required" $nfs.path | quote }}
{{- end -}}

{{- define "convex.dataVolumeMount" -}}
- name: data
  mountPath: {{ include "convex.storageMountPath" . }}
{{- end -}}

{{- define "convex.pgEnv" -}}
{{- $db := .Values.db -}}
- name: DB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "convex.dbSecretName" . }}
      key: {{ default "db-password" $db.passwordRef.key }}
- name: POSTGRES_URL
  value: "postgres://{{ $db.user }}:$(DB_PASSWORD)@{{ required "db.host is required" $db.host }}:{{ $db.port }}"
{{- if eq $db.sslMode "disable" }}
- name: DO_NOT_REQUIRE_SSL
  value: "1"
{{- end }}
{{- end -}}

{{- define "convex.commonBackendEnv" -}}
- name: INSTANCE_NAME
  value: {{ required "instance.name is required" .Values.instance.name | quote }}
- name: INSTANCE_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ include "convex.instanceSecretName" . }}
      key: {{ .Values.instance.secretRef.key }}
{{ include "convex.pgEnv" . }}
- name: CONVEX_CLOUD_ORIGIN
  value: "https://{{ required "hosts.api is required" .Values.hosts.api }}"
- name: CONVEX_SITE_ORIGIN
  value: "https://{{ required "hosts.site is required" .Values.hosts.site }}"
{{- end -}}

{{- define "convex.funrunAddr" -}}
http://{{ include "convex.funrunName" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ trimPrefix ":" .Values.funrun.listen }}
{{- end -}}

{{- define "convex.insightsTokenSecretName" -}}
{{- default (include "convex.instanceSecretName" .) .Values.insights.tokenRef.name -}}
{{- end -}}

{{- define "convex.usageSinkEnv" -}}
- name: CONVEX_USAGE_SINK_URL
  value: {{ required "insights.sinkUrl is required when insights.enabled" .Values.insights.sinkUrl | quote }}
- name: CONVEX_USAGE_SINK_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "convex.insightsTokenSecretName" . }}
      key: {{ .Values.insights.tokenRef.key }}
{{- end -}}
