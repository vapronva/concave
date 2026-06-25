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
{{- with .Values.image.pullSecrets -}}
imagePullSecrets:
  {{- toYaml . | nindent 2 }}
{{- end -}}
{{- end -}}

{{- define "convex.listenPort" -}}
{{- splitList ":" . | last -}}
{{- end -}}

{{- define "convex.podScheduling" -}}
{{- $out := list -}}
{{- with .nodeSelector -}}
{{- $out = append $out (printf "nodeSelector:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- with .tolerations -}}
{{- $out = append $out (printf "tolerations:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- with .topologySpreadConstraints -}}
{{- $out = append $out (printf "topologySpreadConstraints:\n%s" (toYaml . | indent 2)) -}}
{{- end -}}
{{- join "\n" $out -}}
{{- end -}}

{{- define "convex.updateStrategy" -}}
{{- $s := .strategy | default dict -}}
{{- $type := $s.type | default .type -}}
{{- if not (has $type (list "RollingUpdate" "Recreate")) -}}
{{- fail (printf "updateStrategy.type=%q is invalid; use RollingUpdate or Recreate" $type) -}}
{{- end -}}
strategy:
  type: {{ $type }}
  {{- if eq $type "RollingUpdate" }}
  {{- $ru := $s.rollingUpdate | default dict }}
  rollingUpdate:
    maxUnavailable: {{ dig "maxUnavailable" .maxUnavailable $ru }}
    maxSurge: {{ dig "maxSurge" .maxSurge $ru }}
  {{- end }}
{{- end -}}

{{- define "convex.backendDiscoveryLabels" -}}
{{- $ctx := .ctx -}}
{{- $prefix := include "convex.labelPrefix" $ctx -}}
{{ $prefix }}/component: backend
{{ $prefix }}/instance: {{ required "instance.name is required" $ctx.Values.instance.name | quote }}
{{ $prefix }}/role: {{ required "role is required for backend discovery labels" .role }}
{{ $prefix }}/leader-priority: {{ int64 .priority | quote }}
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
  value: "postgres://{{ $db.user }}:$(DB_PASSWORD)@{{ required "db.host is required" $db.host }}:{{ $db.port }}{{- if eq $db.sslMode "disable" }}?sslmode=disable{{- end }}"
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
      key: {{ default "instance-secret" .Values.instance.secretRef.key }}
{{ include "convex.pgEnv" . }}
- name: CONVEX_CLOUD_ORIGIN
  value: "https://{{ required "hosts.api is required" .Values.hosts.api }}"
- name: CONVEX_SITE_ORIGIN
  value: "https://{{ required "hosts.site is required" .Values.hosts.site }}"
{{- with .Values.hosts.dashboard }}
- name: CONVEX_DASHBOARD_ORIGIN
  value: "https://{{ . }}"
{{- end }}
{{- include "convex.controlPlaneEnv" . }}
{{- with .Values.ha.demotionDrainTimeoutSeconds }}
- name: DEMOTION_DRAIN_TIMEOUT_SECS
  value: {{ int64 . | quote }}
{{- end }}
{{- end -}}

{{- define "convex.controlPlaneEnv" -}}
{{- with .Values.controlPlane.tokenSecretRef.name }}
- name: CONVEX_CONTROL_PLANE_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ . }}
      key: {{ default "control-plane-token" $.Values.controlPlane.tokenSecretRef.key }}
{{- end }}
{{- end -}}

{{- define "convex.funrunAddr" -}}
http://{{ include "convex.funrunName" . }}.{{ .Release.Namespace }}.svc.cluster.local:{{ include "convex.listenPort" .Values.funrun.listen }}
{{- end -}}

{{- define "convex.insightsTokenSecretName" -}}
{{- default (include "convex.instanceSecretName" .) .Values.insights.tokenRef.name -}}
{{- end -}}

{{- define "convex.containerSecurityContext" -}}
runAsNonRoot: true
runAsUser: 1001
allowPrivilegeEscalation: false
capabilities:
  drop: ["ALL"]
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "convex.backendContainer" -}}
{{- $ctx := .ctx -}}
- name: backend
  image: {{ include "convex.image" (dict "ctx" $ctx "spec" $ctx.Values.image) }}
  imagePullPolicy: {{ $ctx.Values.image.pullPolicy }}
  ports:
    - { name: cloud, containerPort: 3210 }
    {{- range .extraPorts }}
    - { name: {{ .name }}, containerPort: {{ .containerPort }} }
    {{- end }}
  envFrom:
    - configMapRef:
        name: {{ include "convex.envConfigName" $ctx }}
  env:
    - name: CONVEX_BACKEND_ROLE
      value: {{ .role }}
    {{- if and (eq .role "leader") $ctx.Values.controlPlane.tokenSecretRef.name }}
    - name: CONVEX_BOOT_FOLLOWER_WHEN_INITIALIZED
      value: "true"
    {{- end }}
    {{- include "convex.commonBackendEnv" $ctx | nindent 4 }}
    {{- if $ctx.Values.funrun.enabled }}
    - name: CONVEX_FUNRUN_ADDR
      value: {{ include "convex.funrunAddr" $ctx | quote }}
    - name: POD_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: CONVEX_FUNRUN_CALLBACK_URL
      value: "http://$(POD_IP):3210"
    {{- end }}
    {{- if $ctx.Values.insights.enabled }}
    {{- include "convex.usageSinkEnv" $ctx | nindent 4 }}
    {{- end }}
    {{- with include "convex.sharedTunableEnv" $ctx | trim }}{{ . | nindent 4 }}{{- end }}
    {{- with include "convex.backendTunableEnv" $ctx | trim }}{{ . | nindent 4 }}{{- end }}
    {{- with $ctx.Values.backend.extraEnv }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  volumeMounts:
    {{- include "convex.dataVolumeMount" $ctx | nindent 4 }}
  startupProbe:
    tcpSocket: { port: cloud }
    initialDelaySeconds: {{ .probeInitialDelaySeconds }}
    periodSeconds: 5
    failureThreshold: 32
  readinessProbe:
    httpGet: { path: /readyz, port: cloud }
    periodSeconds: 5
    failureThreshold: 3
  livenessProbe:
    tcpSocket: { port: cloud }
    initialDelaySeconds: 120
    periodSeconds: 20
  {{- with (index $ctx.Values.resources .resourcesKey) }}
  resources:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  securityContext:
    {{- include "convex.containerSecurityContext" $ctx | nindent 4 }}
{{- end -}}

{{- define "convex.usageSinkEnv" -}}
{{- $sink := required "insights.sinkUrl is required when insights.enabled" .Values.insights.sinkUrl -}}
- name: CONVEX_USAGE_SINK_URL
  value: {{ $sink | quote }}
- name: CONVEX_USAGE_SINK_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ include "convex.insightsTokenSecretName" . }}
      key: {{ default "usage-token" .Values.insights.tokenRef.key }}
- name: CONVEX_INSIGHTS_QUERY_URL
  value: {{ required "insights.queryUrl is required when insights.enabled" .Values.insights.queryUrl | quote }}
{{- end -}}

{{- define "convex.sharedTunableEnv" -}}
{{- with .Values.sizeLimits.writeBytes }}
- name: TRANSACTION_MAX_USER_WRITE_SIZE_BYTES
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.sizeLimits.readBytes }}
- name: TRANSACTION_MAX_READ_SIZE_BYTES
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.allocator.arenaMax }}
- name: MALLOC_ARENA_MAX
  value: {{ int64 . | quote }}
{{- end }}
{{- end -}}

{{- define "convex.backendTunableEnv" -}}
{{- with .Values.backend.concurrency.queries }}
- name: APPLICATION_MAX_CONCURRENT_QUERIES
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.backend.concurrency.mutations }}
- name: APPLICATION_MAX_CONCURRENT_MUTATIONS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.backend.concurrency.v8Actions }}
- name: APPLICATION_MAX_CONCURRENT_V8_ACTIONS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.backend.httpTimeoutSeconds }}
- name: HTTP_SERVER_TIMEOUT_SECONDS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.backend.maxConcurrentRequests }}
- name: HTTP_SERVER_MAX_CONCURRENT_REQUESTS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.backend.committerQueueSize }}
- name: COMMITTER_QUEUE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- end -}}

{{- define "convex.funrunTunableEnv" -}}
{{- with .Values.funrun.isolate.maxWorkers }}
- name: MAX_ISOLATE_WORKERS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.isolate.preloadWorkers }}
- name: ISOLATE_PRELOAD_WORKERS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.isolate.heapUserMiB }}
- name: ISOLATE_MAX_USER_HEAP_SIZE
  value: {{ mul (int64 .) 1048576 | quote }}
{{- end }}
{{- with .Values.funrun.isolate.heapExtraMiB }}
- name: ISOLATE_MAX_HEAP_EXTRA_SIZE
  value: {{ mul (int64 .) 1048576 | quote }}
{{- end }}
{{- with .Values.funrun.isolate.arrayBufferMiB }}
- name: ISOLATE_MAX_ARRAY_BUFFER_TOTAL_SIZE
  value: {{ mul (int64 .) 1048576 | quote }}
{{- end }}
{{- with .Values.funrun.isolate.queueSize }}
- name: ISOLATE_QUEUE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.isolate.activeThreads }}
- name: FUNRUN_ISOLATE_ACTIVE_THREADS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.isolate.initialPermitTimeoutMs }}
- name: FUNRUN_INITIAL_PERMIT_TIMEOUT_MS
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.cache.indexBytes }}
- name: FUNRUN_INDEX_CACHE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.cache.followerIndexBytes }}
- name: FUNRUN_FOLLOWER_INDEX_CACHE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.cache.codeBytes }}
- name: FUNRUN_CODE_CACHE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- with .Values.funrun.cache.moduleBytes }}
- name: FUNRUN_MODULE_CACHE_SIZE
  value: {{ int64 . | quote }}
{{- end }}
{{- end -}}
